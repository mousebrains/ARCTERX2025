#
# Write out a dictionary to a NetCDF file
# 
# Jan-2025, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import numpy as np
from netCDF4 import Dataset
from TPWUtils.Thread import Thread
import logging
import queue
import datetime
import os.path
import time
from tempfile import NamedTemporaryFile

class ncWriter(Thread):
    def __init__(self, args:ArgumentParser, ncFilenames:list, varDefs:dict):
        Thread.__init__(self, "NC", args)
        self.__ncFilenames = ncFilenames
        self.__varDefs = varDefs
        self.__queue = queue.Queue()

    def put(self, time:datetime.datetime, record:dict) -> None:
        if not isinstance(time, datetime.datetime):
            logging.info("time %s %s", time, type(time))
            raise ValueError
        self.__queue.put((time, record))

    @staticmethod
    def addArgs(parser:ArgumentParser) -> None:
        grp = parser.add_argument_group(description="ncWriter related options")
        grp.add_argument("--copyTo", type=str, help="Where to copy NetCDF files to atomically")
        grp.add_argument("--bufferSize", type=int, default=1024*1024,
                         help="Read buffer size in bytes for copying to")
        grp.add_argument("--batchDelay", type=int, default=30,
                         help="Seconds to batch up records")

    @staticmethod
    def adjustFilenames(qAdjustFile:list, t:datetime.datetime, filenames):
        adj = []
        t = t.strftime("%Y%m%d")
        for index in range(len(qAdjustFile)):
            if qAdjustFile[index]: 
                adj.append(filenames[index].replace("YYYYMMDD", t))
            else:
                adj.append(filenames[index])
        return adj


    @staticmethod
    def getFillValue(a:str):
        match a:
            case "i8":
                return -9223372036854775808
            case "i4":
                return -2147483648
            case "i2":
                return -32768
            case "u8":
                return 18446744073709551615
            case "u4":
                return 4294967295
            case "u2":
                return 65535
            case "u1":
                return 255
            case _:
                return np.nan if a[0] == "f" else None

    def initializeNC(self, fn:str):
        varDefs = self.__varDefs

        timeName = "time"
        for key in varDefs:
            if key == "global": continue

            item = varDefs[key]
            if "timeName" in item and item["timeName"]:
                timeName = key
                break

        with Dataset(fn, "a") as nc:
            if "global" in varDefs:
                attrs = nc.ncattrs()
                for key in varDefs["global"]:
                    if key not in attrs:
                        nc.setncattr(key, varDefs["global"][key])

            if timeName not in nc.dimensions:
                nc.createDimension(timeName, size=None)
            for name in varDefs:
                if name == "global": continue
                if name in nc.variables: continue
                item = varDefs[name]
                varId = nc.createVariable(name,
                                          datatype=item["type"],
                                          dimensions=(timeName,),
                                          fill_value=self.getFillValue(item["type"]),
                                          )
                for key in item:
                    if key not in ["type", "timeName"]:
                        varId.setncattr(key, item[key])

    def initializeAllNC(self, filenames:list):
        for fn in filenames:
            self.initializeNC(fn)

    def copyTo(self, src:str, tgt:str) -> None:
        stime = time.time()
        sz = self.args.bufferSize
        totSize = 0
        tfn = None
        dirname = os.path.dirname(tgt)
        with NamedTemporaryFile(delete=False, dir=dirname) as ofp, open(src, "rb") as ifp:
            tfn = ofp.name
            buffer = ifp.read(sz)
            while buffer:
                totSize += ofp.write(buffer)
                buffer = ifp.read(sz)
        os.replace(tfn, tgt)

        logging.info("Took %s seconds to copy %s to %s", time.time()-stime, src, tgt)

    def updateNetCDF(self, fn:str, records:list) -> None:
        tRef = None
        with Dataset(fn, "a") as nc:
            varT = nc["time"]
            if "units" not in varT.ncattrs():
                tRef = min(list(map(lambda a: a[0], records)))

                tRef = tRef.replace(microsecond=0)
                varT.setncattr("units", "seconds since " + tRef.strftime("%Y-%m-%dT%H:%M:%S"))
            elif tRef is None:
                units = varT.units.removeprefix("seconds since ")
                tRef = datetime.datetime.strptime(units, "%Y-%m-%dT%H:%M:%S") \
                        .replace(tzinfo=datetime.timezone.utc)
            for (t, record) in records:
                index = round((t - tRef).seconds)
                if index < 0: continue
                nc["time"][index] = index
                for name in record: nc[name][index] = record[name]

    def simplifyRecords(self, records:list) -> list:
        times = {}
        for (t, record) in records:
            dSec = round(t.microsecond / 1000000)
            tNew = t.replace(microsecond=0) + datetime.timedelta(seconds=dSec)
            if tNew not in times:
                times[tNew] = record
            else:
                times[tNew].update(record)

        result = []
        for t in sorted(times):
            result.append((t, times[t]))

        return result

    def runIt(self):
        args = self.args
        q = self.__queue
        delay = args.batchDelay

        ncFiles = list(
                set(
                    map(lambda fn: os.path.abspath(os.path.expanduser(fn)), self.__ncFilenames)
                    )
                )

        if args.copyTo:
            args.copyTo = os.path.abspath(os.path.expanduser(args.copyTo))

        logging.info("Starting %s", ncFiles)

        filesToAdjust = set()
        filesNotToAdjust = set()

        for fn in ncFiles:
            if "YYYYMMDD" in fn:
                filesToAdjust.add(fn)
            else:
                filesNotToAdjust.add(fn)

        if filesNotToAdjust:
            self.initializeAllNC(filesNotToAdjust) # Initialize non-adjustable files

        units = None
        tRef = None

        seenYYYYMMDD = dict()

        while True:
            (t, record) = q.get()
            now = time.time()
            dom = t.day if filesToAdjust else -1
            records = {dom: [(t, record)]}

            while True:
                dt = delay - (time.time() - now)
                if dt <= 0: break
                try:
                    (t, record) = q.get(block=True, timeout=dt)
                    dom = t.day if filesToAdjust else -1
                    if dom in records:
                        records[dom].append((t, record))
                    else:
                        records[dom] = [(t, record)]
                except queue.Empty:
                    break
                except:
                    logging.exception("GotMe")
                    break
         
            toCopy = set()

            for dom in records:
                items = self.simplifyRecords(records[dom])
                if filesToAdjust:
                    yyyymmdd = items[0][0].strftime("%Y%m%d")
                    if yyyymmdd not in seenYYYYMMDD:
                        seenYYYYMMDD[yyyymmdd] = set()
                        for fn in filesToAdjust:
                            fn = fn.replace("YYYYMMDD", yyyymmdd)
                            seenYYYYMMDD[yyyymmdd].add(fn)
                            self.initializeNC(fn)
                    for fn in seenYYYYMMDD[yyyymmdd]:
                        logging.info("Writing %s records to %s", len(items), fn)
                        self.updateNetCDF(fn, items)
                        toCopy.add(fn)
                if filesNotToAdjust:
                    for fn in filesNotToAdjust:
                        logging.info("Writing %s records to %s", len(items), fn)
                        self.updateNetCDF(fn, items)
                        toCopy.add(fn)

            if args.copyTo:
                for fn in toCopy:
                    self.copyTo(fn, os.path.join(args.copyTo, os.path.basename(fn)))

            q.task_done()
