#! /usr/bin/env python3
#
# When things change in directories, copy them to ~/Sync/Shore
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
from TPWUtils.INotify import INotify
from TPWUtils import Logger
import logging
import os.path
import subprocess
import yaml
import time

def rsync(sources:list, args:ArgumentParser) -> bool:
    cmd = [args.rsync,
           "--verbose",
           "--archive",
           "--temp-dir", args.cache,
           ]
    cmd.extend(sources)
    cmd.append(args.target)
    logging.info("cmd %s", cmd)
    sp = subprocess.run(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,)
    try:
        sp.stdout = str(sp.stdout, "utf-8")
    except:
        pass
    if sp.returncode:
        logging.warning("Executing %s\n%s", " ".join(sp.args), sp.stdout)
    elif sp.stdout:
        logging.info("Executed %s\n%s", " ".join(sp.args), sp.stdout)
    else:
        logging.info("Executed %s", " ".join(sp.args))

    return sp.returncode == 0

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--target", type=str, default="~/Sync/Shore", help="Where to rsync into")
parser.add_argument("--config", type=str, 
                    default=os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yaml")),
                    help="Directories to watch")
parser.add_argument("--delay", type=float, default=60, help="Delay after file update to start sync")
parser.add_argument("--rsync", type=str, default="/usr/bin/rsync", help="rsync command to use")
parser.add_argument("--cache", type=str, default="~/.cache", help="rsync --temp-dir")
args = parser.parse_args()

args.cache = os.path.abspath(os.path.expanduser(args.cache))
args.target = os.path.abspath(os.path.expanduser(args.target))

Logger.mkLogger(args)

try:
    with open(args.config, "r") as fp:
        config = yaml.safe_load(fp)

    i = INotify(args)
    i.start()

    for src in config:
        i.addTree(src)

    rsync(config, args)

    q = i.queue
    while True:
        (t0, fn) = q.get()
        q.task_done()
        dt = max(0.1, t0 - time.time() + args.delay)
        logging.info("%s updated, sleeping for %s", fn, dt)
        time.sleep(dt)
        sources = set(fn if os.path.isdir(fn) else os.path.dirname(fn))
        while not q.empty():
            (t0, fn) = q.get()
            sources.add(fn if os.path.isdir(fn) else os.path.dirname(fn))

        rsync(sources, args)

except:
    logging.exception("Unexpected exception")
