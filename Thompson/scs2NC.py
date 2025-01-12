#! /usr/bin/env python3
#
# Process various Sonic wind bow sensor for true speed and direction
#
# April-2023, Pat Welch, pat@mousebrains.com

import logging
import os.path
import glob
import datetime
import math
import re
import time
import numpy as np
import pandas as pd
from mkNC import createNetCDF
from netCDF4 import Dataset
import psycopg
import sys

def mkFilenames(paths:tuple, cur) -> dict:
    patterns = {
        "MET": re.compile(r"^(SONIC-TWIND|PAR|BOW-MET|RAD)-RAW_([0-9]+)-[0-9]+"),
        "NAV": re.compile(r"^CNAV3050-(GGA|VTG)-RAW_([0-9]+)-[0-9]+"),
        "SEAWATER": re.compile(r"^(FLUOROMETER|TSG|SBE38)-RAW_([0-9]+)-[0-9]+"),
        "SOUNDERS": re.compile(r"^(KNUDSEN-PKEL99-RAW|MB-DEPTH)_([0-9]+)-[0-9]+"),
        }

    sql = "SELECT position FROM fileposition WHERE filename=%s;"

    items = {}
    for path in paths:
        for subdir in patterns:
            expr = patterns[subdir]
            for fn in glob.glob(os.path.join(path, subdir, "*.Raw")):
                matches = expr.match(os.path.basename(fn))
                if not matches: continue
                pos = None
                cur.execute(sql, (fn,))
                for row in cur:
                    pos = row[0]
                    break

                if pos and os.path.getsize(fn) == pos: continue
                date = matches[2]
                if date not in items: items[date] = {}
                items[date][fn] = (pos, matches[1])
    return items

def decodeDegMin(degMin:str, direction:str) -> float:
    try:
        degMin = float(degMin)
    except:
        return None

    sgn = -1 if degMin < 0 else 1
    sgn*= -1 if direction.upper() in ("S", "W") else 1
    degMin = abs(degMin)
    deg = math.floor(degMin/100)
    minutes = degMin % 100
    return sgn * (deg + minutes / 60)

def decodeFloat(val:str, norm:float=1.0) -> float:
    try:
        return norm * float(val)
    except:
        return None

def procTWind(fields:tuple) -> dict:
    return {"wSpd": decodeFloat(fields[3]), "wDir": decodeFloat(fields[4])}

def procGGA(fields:tuple) -> dict:
    return {
            "lat": decodeDegMin(fields[4], fields[5]),
            "lon": decodeDegMin(fields[6], fields[7]),
            }

def procVTG(fields:tuple) -> dict:
    return {
            "cog": decodeFloat(fields[3]),
            "sog": decodeFloat(fields[7], 1852/3600), # knots -> meters/sec
            }

def procPAR(fields:tuple) -> dict:
    return {
            "par": decodeFloat(fields[3]),
            }

def procSpeedOfSound(fields:tuple) -> dict:
    return {
            "spdSound": decodeFloat(fields[2]),
            }

def procSBE38(fields:tuple) -> dict:
    return {
            "Tinlet": decodeFloat(fields[2]),
            }

def procTSG(fields:tuple) -> dict:
    return {
            "Ttsg": decodeFloat(fields[2]),
            "cond": decodeFloat(fields[3]),
            "salinity": decodeFloat(fields[4]),
            }

def procFluorometer(fields:tuple) -> dict:
    items = fields[2].split("\t")
    if len(items) < 6: return None
    return {
            "fluorometer": int(items[4]),
            "flThermistor": int(items[5]),
            }

def procBowMet(fields:tuple) -> dict:
    return {
            "Tair": decodeFloat(fields[5]),
            "RH": decodeFloat(fields[6]),
            "Pair": decodeFloat(fields[7]),
            }

def procRadiation(fields:tuple) -> dict:
    return {
            "longWave": decodeFloat(fields[7]),
            "shortWave": decodeFloat(fields[10]),
            }

def procPKEL99(fields:tuple) -> dict:
    if fields[4] != '0': return None
    return {
            "depthKN": decodeFloat(fields[3]),
            }

def procMBdepth(fields:tuple) -> dict:
    return {
            "depthMB": decodeFloat(fields[3]),
            }

codigos = {
        "$TWIND": procTWind,
        "$GPGGA": procGGA,
        "$GPVTG": procVTG,
        "$PPAR": procPAR,
        "$METED": procBowMet,
        "$WIR37": procRadiation,
        "$PKEL99": procPKEL99,
        "$DEPTH": procMBdepth,
        "SBE38": procSBE38,
        "TSG": procTSG,
        "SS": procSpeedOfSound,
        "FLUOROMETER": procFluorometer,
        }

def procLine(line:str, codigo:str=None) -> dict:
    if not line: return None
    fields = line.strip().split(",")
    if len(fields) < 3: return None
    try:
        t = datetime.datetime.strptime(fields[0] + " " + fields[1], "%m/%d/%Y %H:%M:%S.%f")
        tt = t.replace(microsecond=0)
        if t.microsecond >= 500000: tt += datetime.timedelta(seconds=1)
        t  = np.datetime64(t)
        tt = np.datetime64(tt)
        dt = (t - tt).astype("timedelta64[ms]").astype(float) / 1000

        if fields[2][0] == "$": codigo = fields[2]

        if codigo not in codigos:
            logging.warning("Unsupported record type, %s", codigo)
            return None

        val = codigos[codigo](fields)
        return ({"t": tt, "dt": dt} | val) if val else None
    except:
        logging.exception("codigo %s Fields %s", codigo, fields)

def loadFile(fn:str, pos:int, codigo:str) -> tuple:
    items = []
    with open(fn, "r") as fp:
        if pos: fp.seek(pos)
        for line in fp:
            val = procLine(line, codigo)
            if val: items.append(val)
        pos = fp.tell()
    if not items: return (None, pos) # In case there is nothing
    df = pd.DataFrame(items)
    return (df, pos)

def saveDataframe(nc, df:pd.DataFrame, tBase:np.int64) -> None:
    t = (df.t - tBase).astype("timedelta64[s]").astype(np.int64)

    nc.variables["t"][t] = t

    for key in df:
        if key in ["t", "dt"]: continue
        nc.variables[key][t] = df[key].to_numpy()
   
def getTimeOffset(nc) -> np.int64:
    units = nc.variables["t"].getncattr("units")
    since = "since "
    index = units.find(since) + len(since)
    return np.datetime64(units[index:])

def loadIt(paths:list, ncPath:str, dbName:str) -> None:
    sql = "INSERT INTO filePosition VALUES (%s, %s)"
    sql+= " ON CONFLICT (filename) DO UPDATE SET position=EXCLUDED.position;"

    with psycopg.connect(f"dbname={dbName}") as db:
        cur = db.cursor()
        filenames = mkFilenames(paths, cur)

        for date in sorted(filenames):
            ofn = os.path.join(ncPath, f"ship.{date}.nc")
            logging.info("Working on %s %s -> %s", date, len(filenames[date]), ofn)
            frames = []
            cur.execute("BEGIN TRANSACTION;")
            for fn in filenames[date]:
                t0 = time.time()
                (df, pos) = loadFile(fn, filenames[date][fn][0], filenames[date][fn][1])
                t1 = time.time()
                logging.info("Loaded %s in %s secs, sz %s pos %s", 
                             os.path.basename(fn), round(t1-t0,1), df.t.size, pos)
                cur.execute(sql, (fn, pos))
                if df is not None and not df.empty: 
                    frames.append(df)
   
            if not os.path.isfile(ofn):
                tMin = None
                for df in frames:
                    tMin = df.t.min() if tMin is None else min(tMin, df.t.min())
                createNetCDF(ofn, tMin.to_datetime64())

            with Dataset(ofn, "a") as nc:
                tBase = getTimeOffset(nc)
                for df in frames: saveDataframe(nc, df, tBase)

            db.commit()

if __name__ == "__main__":
    from argparse import ArgumentParser
    from TPWUtils import Logger

    parser = ArgumentParser()
    Logger.addArgs(parser)
    parser.add_argument("directory", type=str, nargs="+", help="Directories to look in")
    parser.add_argument("--nc", type=str, required=True, help="Output filename")
    parser.add_argument("--db", type=str, default="arcterx", help="Database name")
    args = parser.parse_args()

    Logger.mkLogger(args, fmt="%(asctime)s %(levelname)s: %(message)s")

    try:
        args.nc = os.path.abspath(os.path.expanduser(args.nc))
        directories = []
        for directory in args.directory:
            directories.append(os.path.abspath(os.path.expanduser(directory)))

        loadIt(directories, args.nc, args.db)
    except:
        logging.exception("GotMe")
