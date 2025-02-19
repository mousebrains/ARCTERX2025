#! /usr/bin/env python3
#
# When things change in directories, copy them to the --target directory
#
# Nov-2024, Pat Welch, pat@mousebrains.com
# Feb-2025, Pat Welch, pat@mousebrains.com allow extra arguments to rsync

from argparse import ArgumentParser
from TPWUtils.INotify import INotify
from TPWUtils import Logger
import logging
import os.path
import subprocess
import yaml
import time

def rsync(src:str, args:ArgumentParser, extras:list=None) -> bool:
    cmd = [args.rsync,
           "--verbose",
           "--archive",
           "--exclude", "~.tmp~",
           "--temp-dir", args.cache,
           ]
    if extras: cmd.extend(extras)
    cmd.append(src)
    cmd.append(args.target)

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

def mkRootPath(fn:str, config:list) -> str:
    rootPaths = list(filter(lambda x: fn.startswith(x), config))
    if rootPaths: return rootPaths[0]
    return fn if os.path.isdir(fn) else os.path.dirname(fn)

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

    for src in config:
        opts = []
        for opt in config[src]:
            if opt: opts.append(opt)
        config[src] = opts if opts else None

    i = INotify(args)
    i.start()

    for src in config:
        i.addTree(src)

    for src in config:
        rsync(src, args, None if not config[src] else config[src])

    q = i.queue
    while True:
        (t0, fn) = q.get()
        q.task_done()
        dt = max(0.1, t0 - time.time() + args.delay)
        logging.info("%s updated, sleeping for %s", fn, dt)
        time.sleep(dt)
        sources = set()
        sources.add(mkRootPath(fn, config))

        while not q.empty():
            (t0, fn) = q.get()
            sources.add(mkRootPath(fn, config))
            logging.info("Adding extra %s", fn)

        for src in sources:
            rsync(src, args, config[src])

except:
    logging.exception("Unexpected exception")
