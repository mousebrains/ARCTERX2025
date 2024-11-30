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

def mkPosition(cur:psycopg.Cursor, tbl:str = "position") -> str:
    sql =f"CREATE TABLE IF NOT EXISTS {tbl}("
    sql+= "  time TIMESTAMP WITH TIME ZONE NOT NULL,"
    sql+= "  name TEXT NOT NULL,"
    sql+= "  latitude NUMERIC, CHECK(latitude >= -90 AND latitude <= 90),"
    sql+= "  longitude NUMERIC, CHECK(longitude >= -180 AND longitude <= 180),"
    sql+= "  qCSV boolean DEFAULT False,"
    sql+= "  qNC boolean DEFAULT False,"
    sql+= "  PRIMARY KEY(time, name)"
    sql+= ");"

    cur.execute(sql)

    return tbl if cur.statusmessage == "CREATE TABLE" else None

def mkFilePosition(cur:psycopg.Cursor, tbl:str="fileposition") -> str:
    sql =f"CREATE TABLE IF NOT EXISTS {tbl}("
    sql+= "  filename TEXT PRIMARY KEY,"
    sql+= "  position BIGINT NOT NULL"
    sql+= ");"

    cur.execute(sql)

    return tbl if cur.statusmessage == "CREATE TABLE" else None

def mkAll(db:str, user:str) -> None:
    dbArg = f"dbname={db} user={user}"

    with psycopg.connect(dbArg) as conn:
        with conn.cursor() as cur:
            beginTransaction(cur)
            mkPosition(cur)
            mkFilePosition(cur)
            conn.commit()

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--db", type=str, default="arcterx", help="Database to work with")
    parser.add_argument("--user", type=str, default="pat", help="Database user to work with")
    args = parser.parse_args()

    mkAll(args.db, args.user)
