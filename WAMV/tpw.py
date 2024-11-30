#! /usr/bin/env python3
#

import socket
import time
from datetime import datetime,timezone

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(1)
addr = ("127.0.0.1", 9061)

for index in range(10):
    now = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    lat = 7.01
    lon = 134.55
    spd = 33
    a = f"{now},{lat},{lon},{spd}"
    sock.sendto(bytes(a, "utf-8"), addr)
    print("Sent", a)

    time.sleep(1)
