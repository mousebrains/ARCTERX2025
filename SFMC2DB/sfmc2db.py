#! /usr/bin/env python3
#
# Monitor SFMC files and add them to the PostgreSQL database
#
# Optionally update a CSV file to be transfer to/from ship
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import psycopg
import MakeTables as mt
from TPWUtils.INotify import INotify
from TPWUtils import Logger
import logging
import os.path
import glob
import time
import pandas as pd
import numpy as np
import MakeTables as mt

def file2DB(fn:str, conn, table:str) -> None:
    (name, ext) = os.path.splitext(os.path.basename(fn))
    device = "slocum"

    sql0 = "SELECT EXTRACT(EPOCH FROM MAX(time)) FROM " + table
    sql0+= " WHERE name=%s AND type=%s;"

    sql1 = "INSERT INTO " + table
    sql1+= " (time,name,type,latitude,longitude)"
    sql1+= " VALUES (%s,'" + name + "','" + device + "',%s,%s)"
    sql1+= " ON CONFLICT DO NOTHING;"

    df = pd.read_csv(fn)
    with conn.cursor() as cur:
        cur.execute(sql0, (name, device))
        tMax = cur.fetchone()[0]
        if tMax is not None:
            df = df[df.time > tMax]
            if df.empty: return
        df.time = df.time.astype("datetime64[s]")
        cur.execute("BEGIN;")
        logging.info("Saving %s rows from %s", df.shape[0], fn)
        for index in range(df.shape[0]):
            row = df.iloc[index]
            cur.execute(sql1, list(row))
        conn.commit()

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("dirs", type=str, nargs="+", help="Directories containing SFMC csv files")
grp = parser.add_argument_group(description="DB related options")
grp.add_argument("--user", type=str, default="pat", help="username to connect as")
grp.add_argument("--DB", type=str, default="arcterx", help="database name to work in")
grp.add_argument("--table", type=str, default="SFMC", help="Table to store data into")
grp = parser.add_argument_group(description="CSV reader related options")
grp.add_argument("--noappend", action="store_true",
                 help="Is the CSV input not file strictly growing?")
args = parser.parse_args()

Logger.mkLogger(args)

dbArg = f"dbname={args.DB} user={args.user}"
mt.mkAll(args.DB, args.user)

i = INotify(args)
i.start()


with psycopg.connect(dbArg) as conn, conn.cursor() as cur:
    mt.beginTransaction(cur)
    for name in args.dirs:
        name = os.path.abspath(os.path.expanduser(name))
        i.addTree(name)
        for fn in glob.glob(os.path.join(name, "*.csv")):
            file2DB(fn, cur, args.table)
    conn.commmit()

q = i.queue
while True:
    (t0, fn) = q.get()
    logging.info("t0 %s fn %s", t0, fn)
    with psycopg.connect(dbArg) as conn, cur=conn.cursor():
        mt.beginTransaction(cur)
        file2DB(fn, cur, args.table)
        conn.commit()
    q.task_done()
