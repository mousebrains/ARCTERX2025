#! /usr/bin/env python3
#
# Record UDP datagrame and save them into a file, no parsing
#
# Jan-2025, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import logging
from TPWUtils import Logger
from TPWUtils.Thread import Thread
import datetime
import queue
import socket
import re
import time
import math
import yaml
from ncWriter import ncWriter
from csvWriter import csvWriter

class Consumer:
    def __init__(self):
        self.queue = queue.Queue()

    def put(self, t:datetime.datetime, body:bytes) -> None:
        self.queue.put((t, body))


class ConsumerNav(Consumer, Thread):
    def __init__(self, args:ArgumentParser, nc:ncWriter, csv:csvWriter):
        Consumer.__init__(self)
        Thread.__init__(self, "NAV", args)
        self.__nc = nc
        self.__csv = csv

    @staticmethod
    def __nemaOk(body:bytes, chksum:bytes) -> bool:
        a = 0
        for c in body[1:]:
            a ^= c
        a &= 0xff
        return (a & 0xff) == int(str(chksum, "utf-8"), 16)

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

    def __ingga(self, t:datetime.datetime, sentence:bytes):
        fields = sentence.split(b",");
        if len(fields) != 15:
            logging.warning("Invalid sentence %s", sentence);
            return
        time = self.__decodeFixTime(
                datetime.datetime.now(tz=datetime.timezone.utc),
                str(fields[1], "utf-8"),
                )
        record = dict(
                lat = self.__decodeDegMin(str(fields[2], "utf-8"), str(fields[3], "utf-8")),
                lon = self.__decodeDegMin(str(fields[4], "utf-8"), str(fields[5], "utf-8")),
                )
        self.__nc.put(time, record)
        self.__csv.put(time, record)

    def __invtg(self, t:datetime.datetime, sentence:bytes):
        fields = sentence.split(b",");
        if len(fields) != 10:
            logging.warning("Invalid sentence %s", sentence);
            return
        record = dict(
                cog = float(fields[1]),
                sog = float(fields[7]),
                )
        self.__nc.put(t, record)
        self.__csv.put(t, record)

    def __hehdt(self, t:datetime.datetime, sentence:bytes):
        fields = sentence.split(b",");
        if len(fields) != 3:
            logging.warning("Invalid sentence %s", sentence)
            return
        record = dict(gyro=float(fields[1]))
        self.__nc.put(t, record)
        self.__csv.put(t, record)

    def runIt(self) -> None:
        args = self.args;
        port = args.navPort
        q = self.queue

        logging.info("Starting port %s", port)

        while True:
            (t, body) = q.get()

            for sentence in body.split():
                fields = re.match(b"^([$][A-Z]+,.+)[*]([0-9A-Fa-f]{2})$", sentence)
                if not fields:
                    logging.warning("No fields found in %s", body)
                    continue
                if not self.__nemaOk(fields[1], fields[2]):
                    logging.warning("NEMA Checksum issue in [1] -> %s [2] -> %s",
                                    fields[1], fields[2])
                    continue
                if fields[1].startswith(b"$INGGA,"):
                    self.__ingga(t, fields[1])
                elif fields[1].startswith(b"$INVTG,"):
                    self.__invtg(t, fields[1])
                elif fields[1].startswith(b"$HEHDT,"):
                    self.__hehdt(t, fields[1])
                else:
                    logging.warning("Not supported %s", fields[1])
            q.task_done()

class ConsumerTSG(Consumer, Thread):
    def __init__(self, args:ArgumentParser, nc:ncWriter, csv:csvWriter):
        Consumer.__init__(self)
        Thread.__init__(self, "TSG", args)
        self.__nc = nc
        self.__csv = csv

    def runIt(self) -> None:
        args = self.args;
        port = args.tsgPort
        q = self.queue

        logging.info("Starting port %s", port)

        while True:
            (t, body) = q.get()
            try:
                body = str(body, "utf-8").strip()
                fields = re.split(r"[\s,]+", body.strip())
                if len(fields) != 6:
                    logging.warning("Bad TSG line, %s", body)
                    continue
                t = datetime.datetime.strptime(fields[0] + " " + fields[1], 
                                               "%d-%m-%Y %H:%M:%S",
                                               ).replace(tzinfo=datetime.timezone.utc)
                record = dict(
                        # temperatureTSG = float(fields[2]),
                        # conductivity = float(fields[3]),
                        salinity = float(fields[4]),
                        # speed_of_sound = float(fields[5]),
                        )
                self.__nc.put(t, record)
                self.__csv.put(t, record)
            except:
                logging.exception("Converting %s to str", body)
            q.task_done()

class ConsumerIntake(Consumer, Thread):
    def __init__(self, args:ArgumentParser, nc:ncWriter, csv:csvWriter):
        Consumer.__init__(self)
        Thread.__init__(self, "Intake", args)
        self.__nc = nc
        self.__csv = csv

    def runIt(self) -> None:
        args = self.args;
        port = args.intakePort
        q = self.queue

        logging.info("Starting port %s", port)

        while True:
            (t, body) = q.get()
            try:
                body = str(body, "utf-8").strip()
                fields = re.split(r"[\s,]+", body.strip())
                if len(fields) != 3:
                    logging.warning("Bady intake line, %s", body)
                    q.task_done()
                    continue
                t = datetime.datetime.strptime(fields[0] + " " + fields[1], 
                                               "%d-%m-%Y %H:%M:%S",
                                               ).replace(tzinfo=datetime.timezone.utc)
                record = dict(
                        temperatureInlet = float(fields[2]),
                        )
                self.__nc.put(t, record)
                self.__csv.put(t, record)
            except:
                logging.exception("Converting %s to str", body)
            q.task_done()

class Listener(Thread):
    def __init__(self, port:int, consumer:Consumer, args:ArgumentParser):
        Thread.__init__(self, f"{port}", args)
        self.__port = port
        self.__consumer = consumer

    def runIt(self):
        q = self.__consumer
        port = self.__port
        logging.info("Starting Listener on %s", port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", port))
        while True:
            (data, addr) = sock.recvfrom(4096)
            t = datetime.datetime.now(tz=datetime.timezone.utc)
            logging.debug("%s %s %s", t, data, addr)
            q.put(t, data)

parser = ArgumentParser()
Logger.addArgs(parser)
ncWriter.addArgs(parser)
csvWriter.addArgs(parser)
parser.add_argument("--config", type=str, default="udp.yaml", help="Variable definition YAML")
parser.add_argument("--navPort", type=int, default=55555, help="UDP port NAV sentence")
parser.add_argument("--tsgPort", type=int, default=55777, help="UDP port for TSG data")
parser.add_argument("--intakePort", type=int, default=55778, help="UDP port for inlet temperatre")
parser.add_argument("nc", type=str, nargs="+", help="NetCDF output filename(s)")
args = parser.parse_args()

Logger.mkLogger(args)

logging.info("Args %s", args)

with open(args.config, "r") as fp: varDefs = yaml.safe_load(fp)
logging.info("Variable Definitions %s", varDefs)

thrds = []
thrds.append(ncWriter(args, args.nc, varDefs))
thrds.append(csvWriter(args))

if args.navPort and args.navPort > 0:
    thrds.append(ConsumerNav(args, thrds[0], thrds[1]))
    thrds.append(Listener(args.navPort, thrds[-1], args))

if args.tsgPort and args.tsgPort > 0:
    thrds.append(ConsumerTSG(args, thrds[0], thrds[1]))
    thrds.append(Listener(args.tsgPort, thrds[-1], args))

if args.intakePort and args.intakePort > 0:
    thrds.append(ConsumerIntake(args, thrds[0], thrds[1]))
    thrds.append(Listener(args.intakePort, thrds[-1], args))

for thrd in thrds:
    thrd.start()

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected")
