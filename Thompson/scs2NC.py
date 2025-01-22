#! /usr/bin/env python3
#
# Process various Sonic wind bow sensor for true speed and direction
#
# April-2023, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import logging
import os
from tempfile import NamedTemporaryFile
import glob
import datetime
import math
import re
import time
import numpy as np
import pandas as pd
import psycopg
import sys
from ncWriter import ncWriter
import yaml

def mkFilenames(paths:tuple, cur) -> dict:
    patterns = {
            "MET": re.compile(r"^(SONIC-TWIND-RAW|PAR-RAW|BOW-MET-RAW|RAD|Campbell-RAD|BRIDGE-WIND-(STBD|PORT)-DRV-Data)_(\d+)-\d+.Raw$"),
            "NAV": re.compile(r"^CNAV3050-(GGA|VTG)-RAW_(\d+)-\d+.Raw$"),
            "SEAWATER": re.compile(r"^(FLUOROMETER|TSG|SBE38)-RAW_(\d+)-\d+.Raw$"),
            "SOUNDERS": re.compile(r"^(KNUDSEN-PKEL99-RAW|MB-DEPTH)_(\d+)-\d+.Raw$"),
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

                if pos is not None and os.path.getsize(fn) == pos: continue
                date = matches[matches.lastindex]
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

def procTWind(fields:tuple, codigo:str) -> dict:
    if "-STBD-" in codigo:
        suffix = "Stbd"
    elif "-PORT-" in codigo:
        suffix = "Port"
    else:
        suffix = ""

    return {"wSpd" + suffix: decodeFloat(fields[3]), 
            "wDir" + suffix: decodeFloat(fields[4])}

def procGGA(fields:tuple, codigo:str) -> dict:
    return {
            "lat": decodeDegMin(fields[4], fields[5]),
            "lon": decodeDegMin(fields[6], fields[7]),
            }

def procVTG(fields:tuple, codigo:str) -> dict:
    return {
            "cog": decodeFloat(fields[3]),
            "sog": decodeFloat(fields[7], 1852/3600), # knots -> meters/sec
            }

def procPAR(fields:tuple, codigo:str) -> dict:
    return {
            "par": decodeFloat(fields[3]),
            }

def procSpeedOfSound(fields:tuple, codigo:str) -> dict:
    return {
            "spdSound": decodeFloat(fields[2]),
            }

def procSBE38(fields:tuple, codigo:str) -> dict:
    return {
            "temperatureInlet": decodeFloat(fields[2]),
            }

def procTSG(fields:tuple, codigo:str) -> dict:
    return {
            "temperatureTSG": decodeFloat(fields[2]),
            "conductivity": decodeFloat(fields[3]),
            "salinity": decodeFloat(fields[4]),
            }

def procFluorometer(fields:tuple, codigo:str) -> dict:
    items = fields[2].split("\t")
    if len(items) < 6: return None
    return {
            "fluorometer": int(items[4]),
            "flThermistor": int(items[5]),
            }

def procBowMet(fields:tuple, codigo:str) -> dict:
    return {
            "temperatureAir": decodeFloat(fields[5]),
            "RH": decodeFloat(fields[6]),
            "pressureAir": decodeFloat(fields[7]),
            }

def procCampbellRadiation(fields:tuple, codigo:str) -> dict:
    return {
            "longWave": decodeFloat(fields[4]),
            "shortWave": decodeFloat(fields[6]),
            }

def procRadiation(fields:tuple, codigo:str) -> dict:
    return {
            "longWave": decodeFloat(fields[7]),
            "shortWave": decodeFloat(fields[10]),
            }

def procPKEL99(fields:tuple, codigo:str) -> dict:
    if fields[4] != '0': return None
    return {
            "depthKN": decodeFloat(fields[3]),
            }

def procMBdepth(fields:tuple, codigo:str) -> dict:
    return {
            "depthMB": decodeFloat(fields[3]),
            }

codigos = {
        "$DEPTH": procMBdepth,
        "$GPGGA": procGGA,
        "$GPVTG": procVTG,
        "$METED": procBowMet,
        "$PKEL99": procPKEL99,
        "$PPAR": procPAR,
        "$RAD": procCampbellRadiation,
        "$TWIND": procTWind,
        "$WIR37": procRadiation,
        "SBE38": procSBE38,
        "TSG": procTSG,
        "SS": procSpeedOfSound,
        "FLUOROMETER": procFluorometer,
        }

def procLine(line:str, codigo:str=None) -> dict:
    codigoOrig = codigo

    if not line: return None
    fields = line.strip().split(",")
    if len(fields) < 3: return None
    try:
        t = datetime.datetime.strptime(fields[0] + " " + fields[1], "%m/%d/%Y %H:%M:%S.%f") \
                .replace(tzinfo=datetime.timezone.utc)
        dSeconds = round(t.microsecond/1000000)
        tt = t.replace(microsecond=0) + datetime.timedelta(seconds=dSeconds)

        if fields[2][0] == "$": codigo = fields[2]

        if codigo not in codigos:
            logging.warning("Unsupported record type, %s, %s", codigo, line.strip())
            return None

        val = codigos[codigo](fields, codigoOrig)
        return ({"t": tt} | val) if val else None
    except:
        logging.exception("codigo %s Fields %s", codigo, fields)

def loadFile(fn:str, pos:int, codigo:str, nc:ncWriter) -> tuple:
    try:
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
    except:
        logging.exception("Working on %s", fn)
        return (None, pos)

def loadIt(paths:list, args:ArgumentParser, nc:ncWriter) -> None:
    dbName = args.db

    sql = "INSERT INTO filePosition VALUES (%s, %s)"
    sql+= " ON CONFLICT (filename) DO UPDATE SET position=EXCLUDED.position;"

    with psycopg.connect(f"dbname={dbName}") as db:
        cur = db.cursor()
        filenames = mkFilenames(paths, cur)
        cur.execute("BEGIN TRANSACTION;")

        for date in sorted(filenames):
            logging.info("Working on %s %s", date, len(filenames[date]))
            frames = None
            for fn in filenames[date]:
                t0 = time.time()
                (df, pos) = loadFile(fn, filenames[date][fn][0], filenames[date][fn][1], nc)
                if df is None:
                    logging.info("No data from %s", fn)
                    if pos is not None:
                        cur.execute(sql, (fn, pos))
                    continue
                if not df.empty:
                    if frames is None:
                        frames = df
                    else:
                        frames = frames.merge(df, how="outer", on="t")
                t1 = time.time()
                logging.info("Loaded %s in %s secs, sz %s pos %s", 
                             os.path.basename(fn), round(t1-t0,1), len(df), pos)
                cur.execute(sql, (fn, pos))
            for row in frames.to_dict(orient="records"):
                t = row["t"].to_pydatetime()
                del row["t"]
                nc.put(t, row)
        db.commit()

if __name__ == "__main__":
    from TPWUtils import Logger
    from TPWUtils.Thread import Thread

    parser = ArgumentParser()
    Logger.addArgs(parser)
    ncWriter.addArgs(parser)
    parser.add_argument("directory", type=str, nargs="+", help="Directories to look in")
    parser.add_argument("--nc", type=str, action="append", required=True, help="Output NetCDF filenames")
    parser.add_argument("--db", type=str, default="arcterx", help="Database name")
    parser.add_argument("--config", type=str, required=True, 
                        help="YAML variable definitions")
    args = parser.parse_args()

    Logger.mkLogger(args)

    try:
        with open(args.config, "r") as fp: varDefs = yaml.safe_load(fp)
        logging.info("varDefs %s", varDefs)

        directories = []
        for directory in args.directory:
            directories.append(os.path.abspath(os.path.expanduser(directory)))

        nc = ncWriter(args, args.nc, varDefs)
        nc.start()

        loadIt(directories, args, nc)

        nc.put(None, None)

        Thread.waitForException()
    except UserWarning:
        logging.info("Finished")
    except:
        logging.exception("GotMe")
