#! /usr/bin/env python3
#
# Create a PostgresSQL table for the asset positions, if needed
#
# Nov-2024, Pat Welch, pat@mousebrains.com

import psycopg
import logging

def beginTransaction(cur:psycopg.Cursor) -> None:
    cur.execute("BEGIN TRANSACTION;")
    
def qTableExists(cur:psycopg.Cursor, tbl:str, schema="public") -> bool:
    sql = "SELECT EXISTS ("
    sql+= "  SELECT FROM pg_tables"
    sql+=f"    WHERE schemaname='{schema}'"
    sql+=f"    AND tablename='{tbl}'"
    sql+= ");"

    cur.execute(sql)

    result = cur.fetchone()
    return result[0] if result else False

def mkPosition(cur:psycopg.Cursor) -> str:
    tbl = "position" # Two places, here and in the sql
    sql = """
-------- 

-- single table for all the positions

CREATE TABLE IF NOT EXISTS position (
  time TIMESTAMP WITH TIME ZONE NOT NULL,
  name TEXT NOT NULL,
  class TEXT NOT NULL,
  latitude NUMERIC, CHECK(latitude >= -90 AND latitude <= 90),
  longitude NUMERIC, CHECK(longitude >= -180 AND longitude <= 180),
  qCSV boolean DEFAULT False,
  qNC boolean DEFAULT False,
  PRIMARY KEY(time, name, class)
);

CREATE INDEX IF NOT EXISTS  pos_class_name ON position (class, name);
CREATE INDEX IF NOT EXISTS  pos_name ON position (name);
CREATE INDEX IF NOT EXISTS  pos_qCSV ON position (qCSV);
CREATE INDEX IF NOT EXISTS  pos_qNC ON position (qNC);

-- Function sends a notification whenever position is updated

CREATE OR REPLACE FUNCTION pos_updated_func() 
  RETURNS  TRIGGER AS $psql$
BEGIN
  PERFORM pg_notify('pos_updated', 'pos');
  RETURN NEW;
end;
$psql$ 
LANGUAGE plpgsql;

-- When position is updated or inserted into, call the nofiication function

CREATE OR REPLACE TRIGGER pos_updated AFTER INSERT OR UPDATE 
  ON position
  FOR EACH STATEMENT
    EXECUTE PROCEDURE pos_updated_func();

--------
    """

    cur.execute(sql)

    return tbl if cur.statusmessage == "CREATE TABLE" else None

def mkFilePosition(cur:psycopg.Cursor) -> str:
    tbl = "fileposition" # Two places, here and the SQL
    sql = """
CREATE TABLE IF NOT EXISTS fileposition (
	filename TEXT PRIMARY KEY,
	position BIGINT NOT NULL
);
    """

    cur.execute(sql)

    return tbl if cur.statusmessage == "CREATE TABLE" else None

def mkAll(db:str, user:str) -> None:
    dbArg = f"dbname={db} user={user}"

    with psycopg.connect(dbArg, autocommit=True) as conn:
        with conn.cursor() as cur:
            mkPosition(cur)
            mkFilePosition(cur)

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--db", type=str, default="arcterx", help="Database to work with")
    parser.add_argument("--user", type=str, default="pat", help="Database user to work with")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)
    mkAll(args.db, args.user)
