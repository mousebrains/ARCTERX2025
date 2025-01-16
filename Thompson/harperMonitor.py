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
import os
import re
import time
import math
from netCDF4 import Dataset

class NetCDFWriter(Thread):
    def __init__(self, args:ArgumentParser):
        Thread.__init__(self, "NC", args)
        self.__queue = queue.Queue()

    def put(self, record:dict) -> None:
        self.__queue.put(record)

    def runIt(self) -> None:
        args = self.args
        q = self.__queue
        logging.info("Starting %s", args.netCDF)

        items = dict(
                time=dict(type="f8", units="seconds since 1970-01-01T00:00:00", calendar="utc"),
                temperatureTSG=dict(type="f4", units="Celsius"),
                conductivity=dict(type="f4", units="V"),
                salinity=dict(type="f4", units="PSU"),
                speed_of_sound=dict(type="f4", units="m/s"),
                latitude=dict(type="f8", units="degrees north"),
                longitude=dict(type="f8", units="degrees east"),
                cog=dict(type="f8", units="degrees true"),
                sog=dict(type="f4", units="km/h"),
                gyro=dict(type="f4", untis="degrees true"),
                temperatureInlet=dict(type="f4", untis="Celsius"),
                )

        while True:
            record = q.get()
            t0 = time.time()

            if "time" not in record:
                logging.warning("Time not in %s", record)
                q.task_done()
                continue

            data = {}
            data[round(record["time"])] = record
    
            while True:
                dt = args.delay - (time.time() - t0)
                if dt <= 0: break
                try:
                    a = q.get(block=True, timeout=dt)
                    if "time" not in record:
                        logging.warning("Time not in %s", record)
                        continue
                    t = round(a["time"])
                    if t in data:
                        data[t].update(a)
                    else:
                        data[t] = a
                except queue.Empty:
                    break

            logging.info("Writeing %s times", len(data))

            with Dataset(args.netCDF, "r+") as nc:
                if "time" not in nc.dimensions:
                    dimID = nc.createDimension("time", None)
                else:
                    dimID = nc.dimensions["time"]

                for t in sorted(data):
                    record = data[t]
                    sz = dimID.size
                    if sz and "time" in nc.variables:
                        for offset in range(1,20):
                            if nc.variables["time"][sz-offset] < t: break
                            if nc.variables["time"][sz-offset] == t:
                                sz = sz - offset
                                break

                    for name in record:
                        if name not in nc.variables:
                            if name in items:
                                item = items[name]
                                varID = nc.createVariable(name, item["type"], ("time",))
                                for attr in item:
                                    if attr != "type": 
                                        varID.setncattr(attr, item[attr])
                            else:
                                nc.createVariable(name, "f4", ("time",))
                        if name == "time":
                            nc.variables[name][sz] = t
                        else:
                            nc.variables[name][sz] = record[name]

            q.task_done()

class Consumer:
    def __init__(self):
        self.queue = queue.Queue()

    def put(self, port:int, t:datetime.datetime, ipv4:str, sport:int, body:bytes) -> None:
        self.queue.put((port, t, ipv4, sport, body))


class ConsumerNav(Consumer, Thread):
    def __init__(self, args:ArgumentParser, nc:NetCDFWriter):
        Consumer.__init__(self)
        Thread.__init__(self, "NAV", args)
        self.__nc = nc

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

    def __ingga(self, sentence:bytes):
        fields = sentence.split(b",");
        if len(fields) != 15:
            logging.warning("Invalid sentence %s", sentence);
            return
        time = self.__decodeFixTime(
                datetime.datetime.now(tz=datetime.timezone.utc),
                str(fields[1], "utf-8"),
                )
        lat = self.__decodeDegMin(str(fields[2], "utf-8"), str(fields[3], "utf-8"))
        lon = self.__decodeDegMin(str(fields[4], "utf-8"), str(fields[5], "utf-8"))
        self.__nc.put(dict(
            time=time.timestamp(),
            latitude=lat,
            longitude=lon,
            ))

    def __invtg(self, sentence:bytes):
        fields = sentence.split(b",");
        if len(fields) != 10:
            logging.warning("Invalid sentence %s", sentence);
            return
        time = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        cog = float(fields[1])
        sog = float(fields[7])
        self.__nc.put(dict(
            time=time,
            cog=cog,
            sog=sog,
            ))

    def __hehdt(self, sentence:bytes):
        fields = sentence.split(b",");
        if len(fields) != 3:
            logging.warning("Invalid sentence %s", sentence)
            return
        self.__nc.put(dict(
            time=datetime.datetime.now(tz=datetime.timezone.utc).timestamp(),
            gyro=float(fields[1]),
            ))

    def runIt(self) -> None:
        args = self.args;
        port = args.navPort
        q = self.queue

        logging.info("Starting port %s", port)

        while True:
            (port, t, ipv4, sport, body) = q.get()
            try:
                output = str(body, "utf-8").strip()
            except:
                logging.error("Converting %s to str", body)
            for sentence in body.split():
                fields = re.match(b"^([$][A-Z]+,.+)[*]([\d[A-Fa-f]{2})$", sentence)
                if not fields:
                    logging.warning("No fields found in %s", body)
                    continue
                if not self.__nemaOk(fields[1], fields[2]):
                    logging.warning("NEMA Checksum issue in [1] -> %s [2] -> %s",
                                    fields[1], fields[2])
                    continue
                if fields[1].startswith(b"$INGGA,"):
                    self.__ingga(fields[1])
                elif fields[1].startswith(b"$INVTG,"):
                    self.__invtg(fields[1])
                elif fields[1].startswith(b"$HEHDT,"):
                    self.__hehdt(fields[1])
                else:
                    logging.warning("Not supported %s", fields[1])
            q.task_done()

class ConsumerTSG(Consumer, Thread):
    def __init__(self, args:ArgumentParser, nc:NetCDFWriter):
        Consumer.__init__(self)
        Thread.__init__(self, "TSG", args)
        self.__nc = nc

    def runIt(self) -> None:
        args = self.args;
        nc = self.__nc;
        port = args.tsgPort
        q = self.queue

        logging.info("Starting port %s", port)

        while True:
            (port, t, ipv4, sport, body) = q.get()
            try:
                body = str(body, "utf-8").strip()
                fields = re.split(r"[\s,]+", body.strip())
                if len(fields) != 6:
                    logging.warning("Bady TSG line, %s", body)
                    continue
                t = datetime.datetime.strptime(fields[0] + " " + fields[1], 
                                               "%d-%m-%Y %H:%M:%S",
                                               ).replace(tzinfo=datetime.timezone.utc).timestamp()
                record = dict(
                        time=t,
                        temperature = float(fields[2]),
                        conductivity = float(fields[3]),
                        salinity = float(fields[4]),
                        speed_of_sound = float(fields[5]),
                        )
                nc.put(record)
            except:
                logging.exception("Converting %s to str", body)
            q.task_done()

class ConsumerIntake(Consumer, Thread):
    def __init__(self, args:ArgumentParser, nc:NetCDFWriter):
        Consumer.__init__(self)
        Thread.__init__(self, "Intake", args)
        self.__nc = nc

    def runIt(self) -> None:
        args = self.args;
        nc = self.__nc;
        port = args.intakePort
        q = self.queue

        logging.info("Starting port %s", port)

        while True:
            (port, t, ipv4, sport, body) = q.get()
            try:
                body = str(body, "utf-8").strip()
                fields = re.split(r"[\s,]+", body.strip())
                if len(fields) != 3:
                    logging.warning("Bady intake line, %s", body)
                    q.task_done()
                    continue
                t = datetime.datetime.strptime(fields[0] + " " + fields[1], 
                                               "%d-%m-%Y %H:%M:%S",
                                               ).replace(tzinfo=datetime.timezone.utc).timestamp()
                record = dict(
                        time=t,
                        temperature_inlet = float(fields[2]),
                        )
                nc.put(record)
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
        logging.info("Starting Listener for %s", port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", port))
        while True:
            (data, addr) = sock.recvfrom(4096)
            t = datetime.datetime.now(tz=datetime.timezone.utc)
            (ipv4, sport) = addr
            q.put(port, t, ipv4, sport, data)
            logging.debug("%s %s %s %s %s", port, t, ipv4, sport, data)

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--navPort", type=int, default=55555, help="UDP port NAV sentence")
parser.add_argument("--tsgPort", type=int, default=55777, help="UDP port for TSG data")
parser.add_argument("--intakePort", type=int, default=55778, help="UDP port for inlet temperatre")
parser.add_argument("--netCDF", type=str, default="./udp.nc", help="Name of output NetCDF file")
parser.add_argument("--delay", type=float, default=5, help="Number of seconds to batch updates")
args = parser.parse_args()

Logger.mkLogger(args)

thrds = []
thrds.append(NetCDFWriter(args))

if args.navPort and args.navPort > 0:
    thrds.append(ConsumerNav(args, thrds[0]))
    thrds.append(Listener(args.navPort, thrds[-1], args))

if args.tsgPort and args.tsgPort > 0:
    thrds.append(ConsumerTSG(args, thrds[0]))
    thrds.append(Listener(args.tsgPort, thrds[-1], args))

if args.intakePort and args.intakePort > 0:
    thrds.append(ConsumerIntake(args, thrds[0]))
    thrds.append(Listener(args.intakePort, thrds[-1], args))

for thrd in thrds:
    thrd.start()

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected")
