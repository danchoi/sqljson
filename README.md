# sqljson

A JSON transformer that adds values from a database


## Usage

Usage: sqljson DBCONN (PROGS | -f FILE)
  Merge decorate JSON with results of SQL queries

The syntax of PROGS is 

    [[keypath]:[replacement key name]:[sql query];]+

DBCONN is a postgresql connection string, e.g. "dbname=mydb"




