#! /usr/bin/env python3
#
# Listen to UDP packets on the Thompson and digest the information
# then store in PostgreSQL, and finally into growing NetCDF files.
#
# April-2023, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import logging
from TPWUtils import Logger
from TPWUtils.Thread import Thread
import datetime
import math
import queue
import re
import psycopg
import socket

class Consumer(Thread):
    def __init__(self, args:ArgumentParser):
        Thread.__init__(self, "CON", args)
        self.__queue = queue.Queue()
        self.__ship = args.ship
        self.__gap = datetime.timedelta(seconds=args.gap)
        self.__tRMC = None
        self.__tGGA = None

    @staticmethod
    def addArgs(parser:ArgumentParser) -> None:
        parser.add_argument("--gap", type=int, default=60, help="Seconds between db updates")
        parser.add_argument("--db", type=str, default="arcterx", help="Database name to work on")
        parser.add_argument("--ship", type=str, default="TGT", help="Vessel name")

    def put(self, port:int, t:datetime.datetime, ipv4:str, sport:int, body:bytes) -> None:
        self.__queue.put((port, t, ipv4, sport, body))

    @staticmethod
    def __decodeDegMin(degmin:str, direction:str) -> float:
        if not degmin: return None
        sgn = -1 if direction.upper() in ["S", "W"] else 1
        degmin = float(degmin)
        sgn *= -1 if degmin < 0 else 1
        degmin = abs(degmin)
        deg = math.floor(degmin / 100)
        minutes = degmin % 100
        return sgn * (deg + minutes / 60)

    @staticmethod
    def __decodeMagVar(degrees:str, direction:str) -> float:
        if not degrees: return None
        sgn = -1 if direction.upper() in ["S", "W"] else 1
        return sgn * float(degrees)

    @staticmethod
    def __decodeFixTime(t:datetime.datetime, tt:str) -> datetime.datetime:
        if not tt: return None
        tt = float(tt)
        h = int(math.floor(tt / 10000))
        m = int(math.floor(tt / 100) % 100)
        s = int(tt % 100)
        tt = t.replace(hour=h, minute=m, second=s, microsecond=0)
        if tt > t: tt -= datetime.timedelta(days=1)
        return tt

    @staticmethod
    def __decodeFixDate(t:datetime.datetime, tt:str) -> datetime.datetime:
        if not tt: return None
        tt = float(tt)
        d = int(math.floor(tt / 10000))
        m = int(math.floor(tt / 100) % 100)
        y = int(tt % 100)
        tt = datetime.datetime.strptime(f"{y}-{m:02d}-{d:02d}", "%y-%m-%d")
        tt = tt.replace(tzinfo=datetime.timezone.utc)
        return tt

    def __dbUpdate(self, db, tFix:datetime.datetime, info:dict) -> None:
        names = ["id", "t"]
        values = [self.__ship, tFix]
        updates = []

        for key in info:
            if info[key] is not None:
                names.append(key)
                values.append(info[key])
                updates.append(key + "=excluded." + key)

        if len(names) < 3: return

        sql = "INSERT INTO ship (" + ",".join(names) + ")"
        sql+= " VALUES (" + ",".join(["%s"] * len(names)) + ")"
        sql+= " ON CONFLICT (t,id) DO UPDATE SET " + ",".join(updates) + ";"
        cur = db.cursor();
        cur.execute("BEGIN TRANSACTION;")
        cur.execute(sql, values)
        db.commit()

    def __RMC(self, port:int, t:datetime.datetime, ipv4:str, sport, fields:list, db) -> None:
        if fields[2] != "A": return # Not active
        info = {}
        tFix = self.__decodeFixTime(t, fields[1])
        if tFix is None: return
        info["lat"] = self.__decodeDegMin(fields[3], fields[4])
        info["lon"] = self.__decodeDegMin(fields[5], fields[6])
        info["sog"] = float(fields[7])
        info["cog"] = float(fields[8])
        dFix = self.__decodeFixDate(t, fields[9])
        info["magVar"] = self.__decodeMagVar(fields[10], fields[11])

        tFix = datetime.datetime.combine(dFix.date(), tFix.time(), tzinfo=datetime.timezone.utc)

        if self.__tRMC and tFix < self.__tRMC: return
        self.__tRMC = tFix + self.__gap # When to write next time

        self.__dbUpdate(db, tFix, info)

    def __GGA(self, port:int, t:datetime.datetime, ipv4:str, sport, fields:list, db) -> None:
        info = {}
        tFix = self.__decodeFixTime(t, fields[1])
        if tFix is None: return

        info["lat"] = self.__decodeDegMin(fields[2], fields[3])
        info["lon"] = self.__decodeDegMin(fields[4], fields[5])
        fixQuality = int(fields[6]) if fields[6] else None
        if fixQuality == 0: return
        info["dilution"] = float(fields[8]) if fields[8] else None
        info["altitude"] = float(fields[9]) if fields[9] else None
        info["height"] = float(fields[11]) if fields[11] else None

        if self.__tGGA and tFix < self.__tGGA: return
        self.__tGGA = tFix + self.__gap # When to write next time

        self.__dbUpdate(db, tFix, info)

    def runIt(self) -> None:
        q = self.__queue
        dbName = self.args.db

        logging.info("Starting db=%s", dbName)

        with psycopg.connect(f"dbname={dbName}") as db:
            while True:
                (port, t, ipv4, sport, body) = q.get()
                q.task_done()
                fields = body.split(",")
                if fields[0].endswith("RMC"):
                    self.__RMC(port, t, ipv4, sport, fields, db)
                elif fields[0].endswith("GGA"):
                    self.__GGA(port, t, ipv4, sport, fields, db)
                elif not re.fullmatch(r"[$]..(VTG|HDT|ZDA)", fields[0]):
                    logging.warning("Unrecognized sentence type %s", fields[0])
                    logging.info("port=%s t=%s addr=%s port=%s body=%s", port, t, ipv4, sport, body)
                    logging.info("%s", fields)

class Listener(Thread):
    def __init__(self, port:int, consumer:Consumer, args:ArgumentParser):
        Thread.__init__(self, f"{port}", args)
        self.__port = port
        self.__consumer = consumer

    @staticmethod
    def __nemaOk(body:bytes, chksum:bytes) -> bool:
        a = 0
        for c in body[1:]:
            a ^= c
        a &= 0xff
        return (a & 0xff) == int(str(chksum, "utf-8"), 16)

    def runIt(self):
        q = self.__consumer
        port = self.__port
        logging.info("Starting Listener for %s", port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", port))
        while True:
            (data, addr) = sock.recvfrom(4096)
            t = datetime.datetime.now(tz=datetime.timezone.utc)
            (ipv4, sport) = addr
            for sentence in data.split():
                fields = re.match(b"^([$][A-Z]+,.+)[*]([\d[A-Fa-f]{2})$", sentence)
                if not fields: continue
                if not self.__nemaOk(fields[1], fields[2]): continue
                # logging.info("port %s t %s ipv4 %s sport %s\n%s", port, t, ipv4, sport, sentence)
                q.put(port, t, ipv4, sport, str(fields[1], "utf-8"))

class Replay(Thread):
    def __init__(self, consumer:Consumer, args:ArgumentParser):
        Thread.__init__(self, "REPLAY", args)
        self.__consumer = consumer

    @staticmethod
    def addArgs(args:ArgumentParser) -> None:
        parser.add_argument("--replay", type=str, help="Filename to read replay records from")

    @staticmethod
    def __nemaOk(body:str, chksum) -> bool:
        a = 0
        for c in bytes(body[1:], "utf-8"):
            a ^= c
        a &= 0xff
        return (a & 0xff) == int(chksum, 16)

    def runIt(self) -> None:
        q = self.__consumer
        fn = self.args.replay
        expr = re.compile(b"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})[.,](\d{3})\s(\d+)\sINFO:\s(\d+[.]\d+[.]\d+[.]\d+)::(\d+) b'(.+)'\s*$")

        logging.info("Starting, %s", fn)

        with open(fn, "rb") as fp:
            for line in fp:
                matches = expr.match(line)
                if not matches:
                    logging.info("Bad line %s", line)
                    continue
                t = datetime.datetime.strptime(str(matches[1], "utf-8"), "%Y-%m-%d %H:%M:%S")
                t += datetime.timedelta(milliseconds=float(str(matches[2], "utf-8")))
                t = t.replace(tzinfo=datetime.timezone.utc)
                port = int(str(matches[3], "utf-8"))
                ipv4 = str(matches[4], "utf-8")
                sport = int(str(matches[5], "utf-8"))
                body = str(matches[6], "utf-8")
                for item in re.split(r"\\[rn]", body):
                    if len(item) < 4: continue
                    fields = re.match(r"^([$][A-Z]+,.+)[*]([\d[A-Fa-f]{2})$", item)
                    if not fields: continue
                    if not self.__nemaOk(fields[1], fields[2]): continue
                    q.put(port, t, ipv4, sport, fields[1])

parser = ArgumentParser()
Logger.addArgs(parser)
Consumer.addArgs(parser)
Replay.addArgs(parser)
parser.add_argument("port", type=int, nargs="+", help="UDP ports to listen to")
args = parser.parse_args()

Logger.mkLogger(args)

thrds = [Consumer(args)]

for port in args.port:
    thrds.append(Listener(port, thrds[0], args))

if args.replay:
    thrds.append(Replay(thrds[0], args))

for thrd in thrds:
    thrd.start()

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected")
