#! /usr/bin/env python3
#
# Copy files from the Thompson's cruise and share directory to ~/Sync/Ship
# which will then be sent to shore
#
# Jan-2025, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import subprocess
import yaml
import os.path
from TPWUtils import Logger
import logging

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("--config", type=str, required=True, help="Input YAML configuration file")
parser.add_argument("--rsync", type=str, default="/usr/bin/rsync", help="Which rsync to use")
args = parser.parse_args()

Logger.mkLogger(args, fmt="%(asctime)s %(levelname)s: %(message)s")

logging.info("Args %s", args)

with open(args.config, "r") as fp: config = yaml.safe_load(fp)
logging.info("Config %s", config)

cmd = [args.rsync]

if "rsync_opts" in config:
    cmd.extend(config["rsync_opts"])

for key in config:
    if key == "rsync_opts": continue
    a = cmd.copy()
    a.append(config[key]["src"])
    a.append(config[key]["tgt"])
    logging.info("%s %s", key, a)

    sp = subprocess.run(a, 
                        shell=False,
                        capture_output=True)

    if sp.returncode:
        logging.info("ReturnCode %s", sp.returncode)

    if sp.stderr:
        try:
            logging.info("STDERR: %s", str(sp.stderr, "utf-8"))
        except:
            logging.info("STDERR: %s", sp.stderr, "utf-8")

    if sp.stdout:
        try:
            logging.info("STDOUT: %s", str(sp.stdout, "utf-8"))
        except:
            logging.info("STDOUT: %s", sp.stdout, "utf-8")
