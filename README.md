# sqljson

A JSON transformer that adds values from a database


## Usage

```
Usage: sqljson DBCONN (PROGS | -f FILE)
  Merge decorate JSON with results of SQL queries
```

The syntax of PROGS is 

    [[keypath]:[replacement key name]:[sql query];]+

DBCONN is a postgresql connection string, e.g. "dbname=mydb"


keypath and replacement key name can be blank, in which case substitutions are made in 
place.

```
$ echo '[6,7,11]' | dist sqljson dbname=mydb "::select title from titles where id = ?" 
[{"title":"Date Movie (Unrated)"},{"title":"Holler Creek Canyon"},{"title":"Freedomland"}]

```

The `-t` flag allows piping in non-JSON, in the form of a plain whitespace
(including newline) delimited text. The output in this case is a stream of
objects, instead of a single top-level JSON object.

```
$ echo '6 7 11' | dist sqljson -t dbname=iw4 "::select title from titles where id = ?" 
{"title":"Date Movie (Unrated)"}
{"title":"Holler Creek Canyon"}
{"title":"Freedomland"}
```

