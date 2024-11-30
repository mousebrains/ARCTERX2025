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
    val = float(val)
    qPos = val < 0
    val = abs(val)
    deg = math.floor(val)
    minutes = (val % 1) * 60
    seconds = (minutes % 1) * 60
    minutes = math.floor(minutes)

    return f"{deg:.0f} {minutes:.0f} {seconds:.2f} " + direction[qPos]

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

Logger.mkLogger(args)

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

with psycopg.connect(dbArg) as conn, conn.cursor() as cur:
    mt.mkPosition(cur)

while True:
    (data, addr) = src.recvfrom(1024) # Buffer size
    print(data, addr)
    if not data: continue
    fields = data.split(b",")
    if len(fields) != 5:
        logging.warning("bad record, %s", data)
        continue
    (time, lat, lon, cog, sog) = fields;
    try:
        time = str(time, "utf-8")
    except:
        continue
    payload = json.dumps(dict(
        name = "WAM-V",
        time = time,
        lat = mkDMS(lat, ("N", "S")),
        lon = mkDMS(lon, ("E", "W")),
        sog = float(sog),
        cog = float(cog),
        ))
    logging.info("Payload %s", payload)
    tgt.sendto(bytes(payload + "\n", "utf-8"), tgtAddr)

    time = datetime.fromisoformat(time).replace(tzinfo=timezone.utc)
    lat = float(lat)
    lon = float(lon)
    with psycopg.connect(dbArg) as conn, conn.cursor() as cur:
        cur.execute("BEGIN TRANSACTION;")
        cur.execute(sql, ("wamv", time, lat, lon))
        conn.commit()

    with open(args.csv, "a") as fp:
        t = time.timestamp()
        fp.write(f"{t:.0f},{lat:.6f},{lon:.6f}\n")



