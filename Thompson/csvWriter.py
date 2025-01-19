#
# Write out a decimated CSV file
# 
# Jan-2025, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import numpy as np
from TPWUtils.Thread import Thread
import logging
import queue
import datetime
import os.path
import time

class csvWriter(Thread):
    def __init__(self, args:ArgumentParser):
        Thread.__init__(self, "CSV", args)
        self.__queue = queue.Queue()

    def put(self, time:datetime.datetime, record:dict) -> None:
        if not isinstance(time, datetime.datetime):
            logging.info("time %s %s", time, type(time))
            raise ValueError
        self.__queue.put((time, record))

    @staticmethod
    def addArgs(parser:ArgumentParser) -> None:
        grp = parser.add_argument_group(description="csvWriter related options")
        grp.add_argument("--csvFilename", type=str,
                         help="Where should a decimated CSV file be saved")
        grp.add_argument("--csvBatch", type=float, default=60,
                         help="Seconds between updates")

    def runIt(self):
        args = self.args
        q = self.__queue
        fn = args.csvFilename
        delay = args.csvBatch

        fn = os.path.abspath(os.path.expanduser(fn)) if fn else fn

        logging.info("Starting %s %s", delay, fn)

        while True:
            (t, record) = q.get()
            if not fn:
                q.task_done()
                continue

            records = record
            now = time.time()
            while True:
                dt = delay - (time.time() - now)
                if dt <= 0: break
                try:
                    (t, record) = q.get(block=True, timeout=dt)
                    records.update(record)
                except queue.Empty:
                    break
                except:
                    logging.exception("GotMe")
                    break

            logging.info("t %s records %s", t, records)
            if "lat" not in records or "lon" not in records:
                q.task_done()
                continue

            seconds = round(t.timestamp() / delay) * delay
            line = [f"{seconds:.0f}"]
            line.append(f"{records['lat']:.6f}")
            line.append(f"{records['lon']:.6f}")
            line.append(f"{records['gyro']:.0f}" if "gyro" in records else "")
            line.append(f"{records['sog']:.1f}" if "sog" in records else "")
            line.append(f"{records['cog']:.0f}" if "cog" in records else "")
            line = ",".join(line)
            logging.info("Line %s", line)

            if not os.path.isfile(fn):
                with open(fn, "w") as fp:
                    fp.write("time,lat,lon,hdg,sog,cog\n")
                    fp.write(line + "\n")
            else:
                with open(fn, "a") as fp:
                    fp.write(line + "\n")

            q.task_done()
