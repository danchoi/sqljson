{-# LANGUAGE OverloadedStrings, StandaloneDeriving, GeneralizedNewtypeDeriving, BangPatterns, FlexibleContexts, MultiParamTypeClasses, ScopedTypeVariables #-}
module Main where

import System.Environment

import Database.HDBC
import Database.HDBC.PostgreSQL
import Data.Convertible 
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy.Char8 as B

import Data.Aeson


-- https://hackage.haskell.org/package/HDBC-postgresql-2.3.2.1/docs/Database-HDBC-PostgreSQL.html
-- http://hackage.haskell.org/package/HDBC-2.2.6.1/docs/Database-HDBC.html

main = do
    [query] <- getArgs
    c <- connectPostgreSQL "dbname=iw4"
    tables <- getTables c
    print tables
    print query
    stmt <- prepare c query
    _ <- execute stmt []
    res <- fetchAllRowsMap' stmt
    B.putStrLn . encode $ res


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


