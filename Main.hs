{-# LANGUAGE OverloadedStrings, StandaloneDeriving, GeneralizedNewtypeDeriving, BangPatterns, FlexibleContexts, MultiParamTypeClasses, ScopedTypeVariables, RecordWildCards #-}
module Main where

import System.Environment

import Database.HDBC
import Database.HDBC.PostgreSQL
import Data.Convertible 
import Data.Text (Text)
import qualified Data.Text.Read as T
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy.Char8 as B
import Control.Applicative
import Data.Maybe
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import Data.Scientific
import Data.Aeson
import Control.Monad (forM, foldM, when)
import Data.Attoparsec.Text hiding (scientific)
import Prelude hiding (takeWhile)
import qualified Options.Applicative as O
import Data.Monoid
import System.IO 
import System.Exit

-- https://hackage.haskell.org/package/HDBC-postgresql-2.3.2.1/docs/Database-HDBC-PostgreSQL.html
-- http://hackage.haskell.org/package/HDBC-2.2.6.1/docs/Database-HDBC.html


data Options = Options {
    plainTextInput :: Bool
  , dbConnString :: String
  , rawPrograms :: Program
  , optDebug :: Bool
  } deriving Show

data Program = RawProgram String | ProgFile FilePath  deriving Show

parseOpts :: O.Parser Options
parseOpts = Options 
    <$> O.flag False True (O.short 't' <> O.help "Plain text input")
    <*> O.argument O.str 
          (O.metavar "DBCONN" <> O.help "postgresql connection string, e.g. \"dbconn=mydb\"")
    <*> (strPrograms <|> progFile)
    <*> O.flag False True (O.short 'd' <> O.help "Debug parsed program")
  where strPrograms = 
          RawProgram <$>  
             O.argument O.str 
                  (O.metavar "PROGS" 
                    <> O.help "patterns and queries to run against the DB and input JSON")
        progFile = ProgFile <$> 
          (O.strOption (O.short 'f' <> O.metavar "FILE" <> O.help "File containing programs"))

opts :: O.ParserInfo Options
opts = O.info (O.helper <*> parseOpts) 
          (O.fullDesc 
            <> O.progDesc "Merge decorate JSON with results of SQL queries"
            <> O.header "sqljson"
            <> O.footer "https://github.com/danchoi/sqljson")

main = do
    Options {..} <- O.execParser opts
    rawPrograms' <- case rawPrograms of 
                      RawProgram s -> return s
                      ProgFile f -> readFile f
    let progs :: [Prog] 
        progs = parseArgs . T.pack $ rawPrograms'
    when optDebug $ do
        print progs
    inputJSON :: Value <- do
          if plainTextInput
          then do 
            -- [Int]
            ids :: [Int] <- (map read . words) `fmap` getContents
            return . Array $ V.fromList $ map (Number . flip scientific 0 . fromIntegral) ids
          else do
            input <- B.getContents
            return $ fromMaybe (error "Could not decode input") (decode input :: Maybe Value)

    c <- connectPostgreSQL dbConnString
    resultJSON <- foldM (runProg c optDebug) inputJSON progs 
    case resultJSON of
      (Array v) | plainTextInput -> 
          -- print as object stream
          mapM_ (B.putStrLn . encode) $ V.toList v
      _ ->  B.putStrLn . encode $ resultJSON
    

runProg :: Connection -> Bool -> Value -> Prog -> IO Value
runProg c debug v prog@(Prog keypath replacement query) = do
    -- hPutStrLn stderr (show prog)
    stmt <- prepare c query
    let conf = Conf keypath replacement stmt debug
    processTop conf v

------------------------------------------------------------------------
-- parse the keypath, replacementKeyName, query

-- syntax is [keypath : replacementName : query [;]? ] which can by repeated

data Prog = Prog KeyPath Text String deriving Show

parseArgs :: Text -> [Prog]
parseArgs x = 
    either (const []) id (parseOnly pProgs x) 

pProgs :: Parser [Prog]
pProgs = pProg `sepBy` char ';'

pProg :: Parser Prog
pProg = Prog
    <$> (pKeyPath <* char ':')
    <*> (pReplacementName <* char ':')
    <*> pSqlStmt

pKeyPath :: Parser KeyPath
pKeyPath = do 
    x <- (reverse . T.splitOn "." . T.strip) <$> takeWhile (notInClass ":")
    if x == [""] 
      then return []
      else return x

pReplacementName :: Parser ReplaceKeyName
pReplacementName = T.strip <$> takeWhile (notInClass ":")

pSqlStmt :: Parser String
pSqlStmt = 
    T.unpack . T.strip <$> takeTill (inClass ";") 

------------------------------------------------------------------------
data Conf = Conf {
      targetKeyPath :: KeyPath 
    , replacementKeyName :: ReplaceKeyName
    , stmt :: Statement
    , debugMode :: Bool
    } 

-- keypath is top parent last because we cons from left
-- A keypath of [""] means take the value itself (a String, Number, or Bool) as the parameter.
type KeyPath = [Text]

-- a replacement name of "" means replace in place
type ReplaceKeyName = Text

processTop :: Conf -> Value -> IO Value
processTop conf v = 
    case v of 
      x@(Object v') -> process conf [] x
      x@(Array v') -> process conf [] x
      x -> error $ "Expected Object or Array, but got " ++ show x

process :: Conf -> KeyPath -> Value -> IO Value
process conf@(Conf {..}) currentKeyPath v = do
    when debugMode $ do
      putStrLn $ "process: currentKeyPath: " ++ show currentKeyPath 
      putStrLn $ "         currentValue: " ++ show v
    case v of 
       (Object hm) -> do
           let pairs = HM.toList hm
           pairs' <- mapM (processPair conf currentKeyPath) pairs
           return . Object . HM.fromList $ pairs'

       (Array vs) | currentKeyPath /= targetKeyPath -> do
          vs' <- mapM (process conf currentKeyPath) . V.toList $ vs
          return . Array . V.fromList $ vs'

       -- possible processing cases
       (Array _)  | currentKeyPath == targetKeyPath -> runSql conf v          
       (String _) | currentKeyPath == targetKeyPath -> runSql conf v 
       _ -> return v

processPair :: Conf -> KeyPath -> (Text, Value) -> IO (Text, Value)
processPair conf@Conf {..} baseKeyPath (k,v) = 
    let k' = if (k:baseKeyPath) == targetKeyPath then replacementKeyName else k
    in (,) <$> pure k' <*> process conf (k:baseKeyPath) v

runSql :: Conf -> Value -> IO Value
runSql conf (Array v) = 
    -- we must assume the vector is of Number ints at this point, since it matches the keypath
    let vs = V.toList $ v
        ints =  catMaybes . map forceToInt $ vs
    in runSqlInts conf ints
runSql conf (String v) = do
    runSqlInts conf $ catMaybes . map ((either (const Nothing) (Just . fst)) . T.decimal) .  T.splitOn "," $ v

forceToInt :: Value -> Maybe Int
forceToInt (Number v) = toBoundedInteger v
forceToInt (String v) = either (const Nothing) (Just . fst) . T.decimal $ v
forceToInt x = error $ "Can't convert to Int: " ++ show x 

runSqlInts :: Conf -> [Int] -> IO Value
runSqlInts Conf {..} xs = do
    xs <- forM xs 
           (\x -> do
              _ <- execute stmt [toSql x]
              res <- fetchAllRowsMap' stmt
              return res) 
    return . toJSON . concat $ xs


--             let xs' = insert xs "titles" (toJSON res )



insert :: Value -> Text -> Value -> Value
insert (Object target) key new = Object $ HM.insert key new target 



instance ToJSON SqlValue where
    toJSON (SqlByteString x) = String . T.decodeUtf8 $ x
    toJSON (SqlInt32 x) = Number $ fromIntegral x
    toJSON (SqlInteger x) = Number $ fromIntegral  x
    toJSON (SqlRational x) = Number $ realToFrac x
    toJSON (SqlDouble x) = Number $ realToFrac x
    toJSON (SqlBool x) = Bool x
    toJSON (SqlLocalTime x) = String . T.pack . show $ x
    toJSON SqlNull = Null
    toJSON x = error $ "Please implement ToJSON instance for SqlValue: " ++ show x

instance Convertible SqlValue Value where
    safeConvert (SqlString x) = return . String . T.pack $ x
    safeConvert (SqlByteString x) = return . String . T.decodeUtf8 $ x
    safeConvert (SqlWord32 x) = return . Number . fromIntegral $ x
    safeConvert (SqlWord64 x) = return . Number . fromIntegral $ x
    safeConvert (SqlInt32 x) = return . Number . fromIntegral $ x
    safeConvert (SqlInt64 x) = return . Number . fromIntegral $ x
    safeConvert (SqlInteger x) = return . Number . fromIntegral $ x
    safeConvert (SqlChar x) = return . String . T.pack $ [x]
    safeConvert (SqlBool x) = return . Bool $ x
    safeConvert (SqlDouble x) = return . Number . realToFrac $ x
    safeConvert (SqlRational x) = return . Number . realToFrac $ x
    safeConvert (SqlNull) = return Null
    safeConvert x = error $ "Please implement Convertible SqlValue Value for " ++ show x
  {-
    You may implement these later:

    SqlLocalDate Day	
    SqlLocalTimeOfDay TimeOfDay	
    SqlZonedLocalTimeOfDay TimeOfDay TimeZone	
    SqlLocalTime LocalTime	
    SqlZonedTime ZonedTime	
    SqlUTCTime UTCTime	
    SqlDiffTime NominalDiffTime	
    SqlPOSIXTime POSIXTime	
  -}


