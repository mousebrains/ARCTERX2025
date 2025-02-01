#
# Write out a dictionary to a NetCDF file
#
# Jan-2025, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import numpy as np
import pandas as pd
from netCDF4 import Dataset
from TPWUtils.Thread import Thread
import logging
import queue
import datetime
import os.path
import time
import sys
from tempfile import NamedTemporaryFile

class ncWriter(Thread):
    def __init__(self, args:ArgumentParser, ncFilenames:list, varDefs:dict):
        Thread.__init__(self, "NC", args)
        self.__ncFilenames = ncFilenames
        self.__varDefs = varDefs
        self.__queue = queue.Queue()

    def join(self):
        self.__queue.join()

    def qsize(self):
        return self.__queue.qsize()

    def put(self, time:datetime.datetime, record:dict) -> None:
        if time is not None and not isinstance(time, datetime.datetime):
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

    def initializeNC(self, fn:str, t0:np.datetime64):
        varDefs = self.__varDefs

        globalOpts = dict(
                compression = "zlib",
                complevel = 5,
                )

        skipNames = ["global", "global_opts"]
        skipKeys = list(globalOpts.keys())
        skipKeys.append("type")
        skipKeys.append("timeName")

        if "global_opts" in varDefs and varDefs["global_opts"]:
            globalOpts.update(varDefs["global_opts"])

        timeName = "time"
        for key in varDefs:
            if key in skipNames: continue
            item = varDefs[key]
            if item and "timeName" in item and item["timeName"]:
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
                if name in skipNames: continue
                if name in nc.variables: continue
                item = varDefs[name]
                opts = globalOpts.copy()
                for key in globalOpts:
                    if key in item:
                        opts[key] = item[key]

                varId = nc.createVariable(name,
                                          datatype=item["type"],
                                          dimensions=(timeName,),
                                          fill_value=self.getFillValue(item["type"]),
                                          **opts,
                                          )
                for key in item:
                    if key not in skipKeys:
                        varId.setncattr(key, item[key])

            if "units" not in nc[timeName].ncattrs():
                nc[timeName].setncattr("units", "seconds since " + t0.strftime("%Y-%m-%dT%H:%M:%S"))

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

    def updateNetCDF(self, fn:str, df:pd.DataFrame, t0:np.datetime64, colNames:list) -> None:
        logging.info("Updating %s rows in %s", df.shape[0], fn)
        stime = time.time()
        with Dataset(fn, "a") as nc:
            varT = nc["time"]
            if "units" in varT.ncattrs():
                units = varT.units.removeprefix("seconds since ")
                tRef = np.datetime64(units)
            else:
                tRef = t0
                varT.setncattr("units", "seconds since " + tRef.strftime("%Y-%m-%dT%H:%M:%S"))

            dt = (t0 - tRef).total_seconds()

            tIndex = (df.tIndex + dt).astype(int)
            qTime = tIndex >= 0
            varT[tIndex[qTime]] = tIndex[qTime]
            for col in colNames:
                val = df[col].values
                q = np.logical_and(qTime, np.logical_not(np.isnan(val)))
                if any(q):
                    nc[col][tIndex[q]] = val[q]
        logging.info("Took %s seconds to update  %s rows in %s", 
                     round(time.time()-stime, 2),
                     df.shape[0],
                     fn)

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

        units = None

        filesInitialized = set()
        qExit = False

        while not qExit:
            (t, record) = q.get()
            if t is None:
                q.task_done()
                logging.warning("t is None for %s", record)
                break

            record["time"] = np.datetime64(t.replace(tzinfo=None))
            records = [record]

            now = time.time()
            while True:
                dt = delay - (time.time() - now)
                if dt <= 0: break
                try:
                    (t, record) = q.get(block=True, timeout=dt)
                    q.task_done()
                    if t is None:
                        logging.warning("t is None for %s", record)
                        qExit = True
                        break
                    record["time"] = np.datetime64(t.replace(tzinfo=None))
                    records.append(record)
                except queue.Empty:
                    break
                except:
                    logging.exception("GotMe")
                    break

            df = pd.DataFrame(records)
            df.time = df.time.dt.round(freq="s")
            df  = df.groupby(by="time", as_index=False).agg("median")
            df = df.sort_values("time")
            t0 = df.time.iloc[0]
            df["tIndex"] = (df.time - t0).astype("timedelta64[s]").astype(int)

            colNames = list(filter(lambda x: x not in ["time", "tIndex"], df.columns))
            toCopy = set()

            if filesToAdjust:
                df["dayOfMonth"] = df.time.dt.floor(freq="D")
                for (dom, rows) in df.groupby(by="dayOfMonth"):
                    yyyymmdd = dom.strftime("%Y%m%d")
                    for fn in filesToAdjust:
                        fn = fn.replace("YYYYMMDD", yyyymmdd)
                        if fn not in filesInitialized:
                            self.initializeNC(fn, rows.time.iloc[0])
                            filesInitialized.add(fn)
                        self.updateNetCDF(fn, rows, t0, colNames)
                        toCopy.add(fn)

            for fn in filesNotToAdjust:
                if fn not in filesInitialized:
                    self.initializeNC(fn, t0)
                    filesInitialized.add(fn)

                self.updateNetCDF(fn, df, t0, colNames)
                toCopy.add(fn)

            if args.copyTo:
                for fn in toCopy:
                    self.copyTo(fn, os.path.join(args.copyTo, os.path.basename(fn)))

            q.task_done()

        raise UserWarning
