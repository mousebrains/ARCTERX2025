#! /usr/bin/env python3
#
# Listen and log datagrams
#
# April-2023, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
from TPWUtils import Thread
from TPWUtils import Logger
import logging
import socket

class Listener(Thread.Thread):
    def __init__(self, port:int, args:ArgumentParser):
        Thread.Thread.__init__(self, f"{port}", args)
        self.__port = port

    def runIt(self):
        port = self.__port
        logging.info("Listener")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", port))
        while True:
            (data, addr) = sock.recvfrom(4096)
            (ipv4, p) = addr
            logging.info("%s::%s %s", ipv4, p, data)

parser = ArgumentParser()
Logger.addArgs(parser)
parser.add_argument("port", nargs="+", type=int)
args = parser.parse_args()

Logger.mkLogger(args)

thrds = []
for port in args.port:
    thrds.append(Listener(port, args))

try:
    for thrd in thrds:
        thrd.start()

    Thread.Thread.waitForException()
except:
    logging.exception("Exception from A")
