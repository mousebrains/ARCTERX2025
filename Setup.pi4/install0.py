#! /usr/bin/env python3
#
# Designed to setup a new system
#
# Dec-2021, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import socket
import logging
import sys
import re
import os
from tempfile import NamedTemporaryFile
import sys
from ExecuteCommand import execCmd

def shutoffAutoupdates(args:ArgumentParser) -> None:
    fn = "/etc/apt/apt.conf.d/20auto-upgrades"
    value = 1 if args.autoupdate else 0
    logging.info("Setting autoupdate to %s, %s", value, fn)
    with open(fn, "r") as ifp, NamedTemporaryFile("w") as ofp:
        for line in ifp.readlines():
            line = re.sub(
                    r'^\s*APT::Periodic::(Update-Package-Lists|Unattended-Upgrade)\s+"\d";\s*',
                    r'APT::Periodic::\1 "' + str(value) + '";\n',
                    line)
            ofp.write(line)
        ofp.flush()
        execCmd((args.sudo, args.cp, ofp.name, fn))

def setupSSH(args:ArgumentParser) -> None:
    sshDir = os.path.abspath(os.path.expanduser("~/.ssh"))

    if not os.path.isdir(sshDir):
        logging.info("Creating %s", sshDir)
        os.makedirs(sshDir, mode=0o700, exist_ok=True)

    fnKey = os.path.join(sshDir, "arcterx")
    if not os.path.isfile(fnKey) or not os.path.isfile(fnKey + ".pub"):
        execCmd((args.sshkeygen, "-t", "ed25519", "-f", fnKey, "-N", ""))
    
    fnConfig = os.path.join(sshDir, "config")
    content = [
            "Host arcterx arcterx.ceoas.oregonstate.edu",
            "  Hostname arcterx.ceoas.oregonstate.edu",
            "  User pat",
            "  IdentityFile ~/.ssh/arcterx",
            "  Compression yes",
            "  ServerAliveInterval 120",
            "  ServerAliveCountMax 10",
            ]
    if os.path.isfile(fnConfig):
        logging.info("Reading existing %s", fnConfig)
        qIgnore = False
        with open(fnConfig, "r") as fp:
            for line in fp.readlines():
                if re.match(r"\s*Host\s+", line):
                    qIgnore = re.match(r"\s*Host\s+arcterx\s*", line) is not None
                if qIgnore: continue
                content.append(line.rstrip())
    logging.info("Updating %s", fnConfig)
    with open(fnConfig, "w") as fp:
        fp.write("\n".join(content))
        fp.write("\n")

    execCmd((args.sshcopyid, "-i", fnKey, "arcterx"))

parser = ArgumentParser()
grp = parser.add_argument_group(description="installation options")
grp.add_argument("--gitUser", type=str, default="Pat Welch", help="Git username")
grp.add_argument("--gitemail", type=str, default="pat@mousebrains.com", help="Git email")
grp.add_argument("--gitEditor", type=str, default="vim", help="Git editor")
grp = parser.add_argument_group(description="Logging options")
grp.add_argument("--verbose", action="store_true", help="Enable INFO messages")
grp.add_argument("--debug", action="store_true", help="Enable INFO+DEBUG messages")
grp.add_argument("--logDirectory", type=str, default="~/logs",
                 help="Where logfiles are stored")
grp.add_argument("--venv", type=str, default="~/.venv",
                 help="Path to my python virtual environment.")

grp = parser.add_mutually_exclusive_group()
grp.add_argument("--shore", action="store_true", help="This is a shore side installation")
grp.add_argument("--ship", action="store_true", help="This is a ship side installation")
grp = parser.add_argument_group(description="syncthing arguments")
grp.add_argument("--folderRoot", type=str, default="~/Sync.ARCTERX", help="Sync thing root")
grp.add_argument("--kbpsSend", type=int, default=25, help="kilobytes/sec to send data")
grp.add_argument("--kbpsRecv", type=int, default=25, help="kilobytes/sec to recveive data")
grp.add_argument("--peer", type=str, default="arcterx", help="syncthing peer")

grp = parser.add_argument_group(description="Flow options")
grp.add_argument("--noUpgrade", action="store_true",
        help="Don't do system update/upgrade/autoremove")
grp.add_argument("--autoupdate", action="store_true", help="Don't turn off autoupdates")
grp.add_argument("--noreboot", action="store_true", help="Don't reboot the system at the end")

grp = parser.add_argument_group(description="Commands")
grp.add_argument("--sudo", type=str, default="/usr/bin/sudo", help="Full path to sudo")
grp.add_argument("--apt", type=str, default="/usr/bin/apt-get", help="Full path to apt-get")
grp.add_argument("--cp", type=str, default="/usr/bin/cp", help="Full path to cp")
grp.add_argument("--touch", type=str, default="/usr/bin/touch", help="Full path to touch")
grp.add_argument("--python", type=str, default="/usr/bin/python3", help="Full path to python3")
grp.add_argument("--git", type=str, default="/usr/bin/git", help="Full path to git")
grp.add_argument("--sshkeygen", type=str, default="/usr/bin/ssh-keygen",
    help="Full path to ssh-keygen")
grp.add_argument("--sshcopyid", type=str, default="/usr/bin/ssh-copy-id",
    help="Full path to ssh-copy-id")
args = parser.parse_args()

args.folderRoot = os.path.abspat(os.path.expanduser(args.folderRoot))
args.logDirectory = os.path.abspat(os.path.expanduser(args.logDirectory))
args.venv = os.path.abspat(os.path.expanduser(args.venv))

if args.verbose:
    logging.basicConfig(level=logging.INFO)
elif args.debug:
    logging.basicConfig(level=logging.DEBUG)

if not args.noUpgrade: # System level update, upgrade, and remove old packages
    execCmd((args.sudo, args.apt, "update"))
    execCmd((args.sudo, args.apt, "--yes", "upgrade"))
    execCmd((args.sudo, args.apt, "--yes", "autoremove"))

shutoffAutoupdates(args) # Turn off auto updates
execCmd((args.sudo, args.touch, "/etc/cloud/cloud-init.disabled"))

# Set up git global variables
for key, value in {"user.name": args.gitUser, "user.email": args.gitemail,
        "core.editor": args.gitEditor, "pull.rebase": "false", 
        "submodule.recurse": "true", "diff.submodule": "log",
        "status.submodulesummary": "1", "push.recurseSubmodules": "on-demand",
        }.items():
    execCmd((args.git, "config", "--global", key, value))

# Install system packages I want
execCmd((
    args.sudo,
    args.apt,
    "--yes",
    "install",
    "fail2ban", 
    "nginx", 
    "php-fpm", 
    "samba*",
    "php-xml",
    "php-yaml",
    "python3-pip",
    "python3-pandas",
    "python3-xarray",
    "python3-geopandas",
    "python3-venv",
    ))

execCmd(("python3", 
         "-m", "venv", 
         "--system-site-packages",
         args.venv,
        )

# Install python packages
for pkg in ("inotify-simple", "libais"):
    execCmd((
        os.path.join(args.venv, "bin/python3"),
        "-m", "pip",
        "install",
        pkg,
        ))

for name in (args.logDirectory,
             os.path.join(args.folderRoot, "Ship"), 
             os.path.join(args.folderRoot, "Shore"),
             ):
    name = os.path.abspath(os.path.expanduser(name))
    if not os.path.isdir(name):
        logging.info("Creating %s", name)
        os.makedirs(name, exist_ok=True, mode=0o755)

if args.shore:
    logging.info("Nothing to do for shoreside")
    # execCmd(("./install.py", "--folderRoot", args.folderRoot, "--shore",),
            # cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "../syncthing")),
            # )
else: # Shipside
    setupSSH(args) # Setup ssh for connecting to arcterx
    # Set up reverse ssh tunnel so shoreside can log into this machine
    execCmd(("./install.py",), 
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "../SSHTunnel")),
            )
    # execCmd(("./install.py", "--folderRoot", args.folderRoot, "--ship",
        # "--kbpsSend", str(args.kbpsSend), "--kbpsRecv", str(args.kbpsRecv), "--peer", args.peer),
        # cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "./syncthing")))

if not args.noreboot:
    execCmd((args.sudo, "reboot"))
