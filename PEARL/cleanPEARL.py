#! /usr/bin/env python3
#
# Clean up the PEARL drifter dataset for some injected points from the future.
#
# Jan-2025, Pat Welch

from argparse import ArgumentParser
import pandas as pd
import os.path
from TPWUtils.Thread import Thread
from TPWUtils.INotify import INotify
from TPWUtils import Logger
import logging
import queue
import time

class Cleaner(Thread):
    def __init__(self, args:ArgumentParser, q:queue.Queue) -> None:
        Thread.__init__(self, "Cleaner", args)
        self.__queue = q

    def runIt(self) -> None:
        args = self.args
        q = self.__queue
        logging.info("Starting %s -> %s", args.src, args.dest)

        while True:
            (t0, fn) = q.get()

            if fn != args.src:
                q.task_done()
                continue

            time.sleep(args.delay)

            tbl = pd.read_csv(args.src)
            logging.info("Read %s rows from %s", tbl.shape[0], args.src)

            columns = list(tbl.columns)
            tbl.rename(columns={columns[-1]: "temperature"}, inplace=True)
            logging.info("tbl %s", tbl)

            tbl.drop_duplicates(subset=("imei", "long", "lat"),
                                keep="last", 
                                ignore_index=True, 
                                inplace=True)
            logging.info("Cleaned to %s rows", tbl.shape[0])

            tbl.lat = round(tbl.lat, 6)
            tbl.long = round(tbl.long, 6)
            tbl.sort_values(["timestamp", "imei"],
                            kind="stable",
                            ignore_index=True,
                            inplace=True);

            if os.path.isfile(args.dest):
                dest = pd.read_csv(args.dest)
                if not dest.equals(tbl):
                    tbl.to_csv(args.dest, index=False)
                    logging.info("Updated %s", args.dest)
                else:
                    logging.info("No need to update %s", args.dest)
            else:
                tbl.to_csv(args.dest, index=False)
                logging.info("Wrote %s", args.dest)
            q.task_done()

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--delay", type=float, default=10, help="Delay after src update before loading")
parser.add_argument("src", type=str, help="Input CSV file")
parser.add_argument("dest", type=str, help="Output cleaned CSV file")
args = parser.parse_args()

Logger.mkLogger(args)

args.src = os.path.abspath(os.path.expanduser(args.src))
args.dest = os.path.abspath(os.path.expanduser(args.dest))

i = INotify(args)
rdr = Cleaner(args, i.queue)
i.start()
rdr.start()
i.addTree(os.path.dirname(args.src))

try:
    Thread.waitForException()
except:
    logging.execption("Unexpected exception, %s", args)
