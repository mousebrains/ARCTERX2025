#! /usr/bin/env python3
#
# Clean up the PEARL drifter dataset for some injected points from the future.
#
# Jan-2025, Pat Welch

from argparse import ArgumentParser
import os.path
from TPWUtils.Thread import Thread
from TPWUtils.INotify import INotify
from TPWUtils import Logger
import logging
import queue
import time
import subprocess

class Builder(Thread):
    def __init__(self, args:ArgumentParser, q:queue.Queue) -> None:
        Thread.__init__(self, "GEN", args)
        self.__queue = q

    def runIt(self) -> None:
        args = self.args
        q = self.__queue
        logging.info("Starting %s -> %s", args.source, args.destination)

        nameMap = dict(
                boomer="mkBoomer",
                starbuck="mkStarbuck",
                catalina="mkCatalina",
                mariner="mkMariner",
                osu684="mkOSU684",
                osu685="mkOSU685",
                SFMC=None,
                )

        while True:
            (t, fn) = q.get()
            logging.info("t %s fn %s", t, fn)

            commands = set()

            dirname = os.path.basename(os.path.dirname(fn))

            if dirname not in nameMap:
                logging.info("Unsupported %s", dirname)
                q.task_done()
                continue

            if nameMap[dirname] is None:
                q.task_done()
                continue

            commands.add(nameMap[dirname])

            now = time.time()
            while True:
                dt = args.delay - (time.time() - now)
                if dt <= 0: break
                logging.info("Sleeping for %s seconds", dt)
                try:
                    (t, fn) = q.get(block=True, timeout=dt)
                    logging.info("t %s fn %s", t, fn)
                    dirname = os.path.basename(os.path.dirname(fn))
                    if dirname not in nameMap:
                        logging.info("Unsupported %s", dirname)
                        continue
                    commands.add(nameMap[dirname])
                except:
                    pass

            for name in commands:
                cmd = (os.path.join(args.destination, name))
                logging.info("Executing %s", cmd)
                sp = subprocess.run(cmd, shell=False, capture_output=True)

                if sp.returncode:
                     logging.info("ReturnCode: %s", sp.returncode)

                if sp.stdout:
                    a = sp.stdout
                    try:
                        a = str(a, "utf-8")
                    except:
                        pass
                    logging.info("STDOUT %s", a)

                if sp.stderr:
                    a = sp.stderr
                    try:
                        a = str(a, "utf-8")
                    except:
                        pass
                    logging.info("STDERR %s", a)

            q.task_done()

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--delay", type=float, default=60, help="Delay after src update before loading")
parser.add_argument("--source", type=str, default="~/Sync/Shore/SFMC",
                    help="Input SFMC directory root")
parser.add_argument("--destination", type=str, default="~/Sync/Processed/SFMC",
                    help="Output directory for generated NetCDF files")
parser.add_argument("--command", type=str, default="mkAll",
                    help="Command to execute in destination directory to rebuild NetCDF files")
args = parser.parse_args()

Logger.mkLogger(args)

args.source = os.path.abspath(os.path.expanduser(args.source))
args.destination = os.path.abspath(os.path.expanduser(args.destination))

i = INotify(args)
rdr = Builder(args, i.queue)
i.start()
rdr.start()
i.addTree(args.source)

try:
    Thread.waitForException()
except:
    logging.execption("Unexpected exception, %s", args)
