#! /usr/bin/env python3
#
# Extract the Nautilus' position information
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import MakeTables as mktbl
from TPWUtils import Logger
import logging
import glob
import os
import json
import psycopg
import time
import numpy as np
from netCDF4 import Dataset
from cftime import date2num
from datetime import datetime, timezone, timedelta

def processFile(fn:str, cur) -> bool:
    sql = "SELECT position FROM fileposition WHERE filename=%s;"
    cur.execute(sql, (fn,))
    prevPos = cur.fetchone()
    if prevPos is not None and os.path.getsize(fn) == prevPos[0]: 
        logging.info("No need to do anything for %s", fn)
        return False

    logging.info("Processing %s prev %s %s", fn, prevPos, os.path.getsize(fn))
    sql = "INSERT INTO position (time,name,latitude,longitude) VALUES (%s,%s,%s,%s)"
    sql+= " ON CONFLICT DO NOTHING;"
    sql1 = "INSERT INTO fileposition (filename,position)"
    sql1+= " VALUES (%s,%s)"
    sql1+= " ON CONFLICT (filename) DO UPDATE SET position=EXCLUDED.position;"
    mktbl.beginTransaction(cur)
    with open(fn, "r") as fp:
        if prevPos is not None:
            fp.seek(prevPos[0]);
        for line in fp:
            fields = line.split("\t")
            if len(fields) < 4: continue
            info = json.loads(fields[3])
            t = datetime.fromtimestamp(info["time"], tz=timezone.utc)
            cur.execute(sql, (t, "nautilus", info["latitude"], info["longitude"]))
        cur.execute(sql1, (fn, fp.tell()))
    cur.connection.commit()
    return True

def updateCSV(cur, fn:str, spacing:str="minute", name:str="nautilus", tbl:str="position") -> None:
    sql = "WITH tpw AS ("
    sql+=f"UPDATE {tbl} SET qCSV=True WHERE qCSV=False AND name=%s"
    sql+= " RETURNING time,latitude,longitude)"
    sql+= "SELECT date_trunc(%s, time) AS t, avg(latitude) AS lat, avg(longitude) AS lon"
    sql+= " FROM tpw GROUP BY t ORDER BY t;"

    cur.execute(sql, (name, spacing))

    if not os.path.isfile(fn):
        dirCSV = os.path.dirname(fn)
        if not os.path.isdir(dirCSV):
            logging.info("Creating %s", dirCSV)
            os.makedirs(dirCSV, 0o755, exist_ok=True)

        with open(fn, "w") as fp:
            fp.write("time,lat,lon\n");

    with open(fn, "a") as fp:
        for rec in cur:
            (t, lat, lon) = rec
            t = t.timestamp()
            fp.write(f"{t:.0f},{lat:.6f},{lon:.6f}\n")

def updateNetCDF(cur, fn:str, spacing:str="second", name="nautilus", tbl="position") -> None:
    sql = "WITH tpw AS ("
    sql+=f"UPDATE {tbl} SET qNC=True WHERE qNC=False AND name=%s"
    sql+= " RETURNING time,latitude,longitude)"
    sql+= "SELECT date_trunc(%s, time) AS t, avg(latitude) AS lat, avg(longitude) AS lon"
    sql+= " FROM tpw GROUP BY t ORDER BY t;"

    cur.execute(sql, (name, spacing))
    rows = cur.fetchall()
    if not rows: return

    dirNC = os.path.dirname(fn)
    if not os.path.isdir(dirNC):
        logging.info("Creating %s", dirNC)
        os.makedirs(dirNC, 0o755, exist_ok=True)

    with Dataset(fn, "r+", format="NETCDF4") as nc:
        if "time" not in nc.dimensions:
            nc.createDimension("time", None)
        if "time" not in nc.variables: 
            tVar = nc.createVariable("time", "f8", ("time",), compression="zlib")
            tVar.units = "seconds since 1970-01-01 00:00:00"
            tVar.calendar = "proleptic_gregorian"
        if "lat" not in nc.variables: 
            latVar = nc.createVariable("lat", "f8", ("time",), compression="zlib")
            latVar.long_name = "latitude" ;
            latVar.units = "degrees_north" ;
            latVar.standard_name = "latitude" ;
        if "lon" not in nc.variables: 
            lonVar = nc.createVariable("lon", "f8", ("time",), compression="zlib")
            lonVar.long_name = "longitude" ;
            lonVar.units = "degrees_east" ;
            lonVar.standard_name = "longitude" ;

        time = []
        lat = []
        lon = []
        for row in rows:
            time.append(row[0].timestamp())
            lat.append(row[1])
            lon.append(row[2])

        time = np.array(time).astype("datetime64[s]")
        lat = np.array(lat).astype(float)
        lon = np.array(lon).astype(float)

        n = len(nc.dimensions["time"])
        nc.variables["time"][n:] = time
        nc.variables["lat"][n:] = lat
        nc.variables["lon"][n:] = lon
        logging.info("Saved %s rows from %s to %s", time.shape[0], time[0], time[-1])

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--srcGlob", type=str, default="*.SPAFRM",
                    help="Glob pattern for position information")
parser.add_argument("--srcDir", type=str,
                    default="/nautilus/cruise/raw/datalog",
                    help="Where Nautilus' position files are kept")
parser.add_argument("--csv", type=str, default="~/Sync/Ship/ship/nautilus.csv",
                    help="Where to save monotonically growing CSV files")
parser.add_argument("--netcdf", type=str, default="~/Sync/Processed/ship/gps.nc",
                    help="Where to save ship positions to")
parser.add_argument("--db", type=str, default="arcterx", help="Database to work with")
parser.add_argument("--user", type=str, default="pat", help="Database user to work with")
parser.add_argument("--dt", type=float, default=10*60, help="Seconds between checking for new data")
parser.add_argument("--spacingCSV", type=str, default="minute",
                    choices=("microseconds", "milliseconds", "second", "minute", "hour",
                             "day", "week", "month", "quarter", "year", "decade", "century",
                             "millenium"),
                    help="Spacing between CSV records")
parser.add_argument("--spacingNC", type=str, default="second",
                    choices=("microseconds", "milliseconds", "second", "minute", "hour",
                             "day", "week", "month", "quarter", "year", "decade", "century",
                             "millenium"),
                    help="Spacing between NC records")
args = parser.parse_args()

args.srcDir = os.path.abspath(os.path.expanduser(args.srcDir))
args.csv    = os.path.abspath(os.path.expanduser(args.csv))
args.netcdf = os.path.abspath(os.path.expanduser(args.netcdf))

srcPattern = os.path.join(args.srcDir, args.srcGlob)
dbArg = f"dbname={args.db} user={args.user}"

Logger.mkLogger(args)

mktbl.mkAll(args.db, args.user)

while True:
    t0 = time.time()
    with psycopg.connect(dbArg) as conn, conn.cursor() as cur:
        for fn in sorted(glob.glob(srcPattern)):
            processFile(fn, cur)
        updateCSV(cur, args.csv, args.spacingCSV)
        updateNetCDF(cur, args.netcdf, args.spacingNC)
    now = time.time()
    dt = max(t0 + args.dt - now, 10)
    logging.info("Sleeping for %s", dt)
    time.sleep(dt)

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected exception")
