#! /usr/bin/env python3
#
# Listen for WAMV positions
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import socket
from datetime import datetime, timezone
from TPWUtils import Logger
import logging
import json
import math
import psycopg
import os
import MakeTables as mt

def mkDMS(val:bytes, direction:tuple[str]) -> str:
    try:
        val = float(val)
        qPos = val < 0
        val = abs(val)
        deg = math.floor(val)
        minutes = (val % 1) * 60
        seconds = (minutes % 1) * 60
        minutes = math.floor(minutes)

        return f"{deg:.0f} {minutes:.0f} {seconds:.2f} " + direction[qPos]
    except:
        return None

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--csv", type=str, default="~/Sync/Ship/WAMV/wamv.csv", 
                    help="CSV filename")
parser.add_argument("--db", type=str, default="arcterx", help="DB name")
parser.add_argument("--username", type=str, default="pat", help="DB username")
parser.add_argument("--port", type=int, default=9061, help="Listener port")
parser.add_argument("--tgtHost", type=str, default="10.1.100.21",
                    help="Where to forward datagrams to");
parser.add_argument("--tgtPort", type=int, default="31337",
                    help="Where to forward datagrams to");
args = parser.parse_args()

Logger.mkLogger(args, fmt="%(asctime)s %(levelname)s: %(message)s")

args.csv = os.path.abspath(os.path.expanduser(args.csv))
csvDir = os.path.dirname(args.csv)
if not os.path.isdir(csvDir):
    logging.info("Creating %s", csvDir)
    os.makedirs(csvDir, 0o755, exist_ok=True)

if not os.path.isfile(args.csv):
    with open(args.csv, "w") as fp:
        fp.write("time,lat,lon\n")

src = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
src.bind(("", args.port))

tgt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
tgt.settimeout(1)
tgtAddr = (args.tgtHost, args.tgtPort)

dbArg = f"dbname={args.db} user={args.username}"
sql = "INSERT INTO position (name, time, latitude, longitude) VALUES (%s,%s,%s,%s)"
sql+= " ON CONFLICT DO NOTHING;"

logging.info("Starting");

with psycopg.connect(dbArg, autocommit=True) as conn, conn.cursor() as cur:
    mt.mkPosition(cur)

while True:
    (data, addr) = src.recvfrom(1024) # Buffer size
    logging.info("data %s addr %s", data, addr)
    if not data: continue
    fields = data.split(b",")
    if len(fields) != 5:
        logging.warning("bad record, %s", data)
        continue
    (time, lat, lon, cog, sog) = fields;
    time = time.strip()
    lat = lat.strip()
    lon = lon.strip()
    cog = cog.strip()
    sog = sog.strip()

    logging.info("time %s lat %s lon %s cog %s sog %s", time, lat, lon, cog, sog)

    if not len(time) or not len(lat) or not len(lon): continue

    try:
        time = str(time, "utf-8")
    except:
        logging.warning("Unable to convert %s to a string", time)
        continue

    try:
        payload = dict(
            name = "WAM-V",
            time = time,
            lat = mkDMS(lat, ("N", "S")),
            lon = mkDMS(lon, ("E", "W")),
            sog = float(sog),
            cog = float(cog),
            )
        logging.info("Payload %s", payload)
        if payload["lat"] is not None and payload["lon"] is not None: 
            tgt.sendto(bytes(json.dumps(payload) + "\n", "utf-8"), tgtAddr)
    except:
        logging.exception("Failed in payload and send")

    try:
        time = datetime.fromisoformat(time).replace(tzinfo=timezone.utc)
        lat = float(lat)
        lon = float(lon)
    except:
        logging.exception("Failed in conversion")
        continue

    try:
        with psycopg.connect(dbArg, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(sql, ("wamv", time, lat, lon))
    except:
        logging.exception("Failed in DB update")

    try:
        with open(args.csv, "a") as fp:
            t = time.timestamp()
            fp.write(f"{t:.0f},{lat:.6f},{lon:.6f}\n")
    except:
        logging.exception("Failed in writing CSV")



