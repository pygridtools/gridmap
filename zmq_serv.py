#! /usr/bin/env python
# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2010-2012 Christian Widmer
# Copyright (C) 2010-2012 Max-Planck-Society

"""
simple example to test if zmq is working correctly.

first open a shell and execute the server:
python zmq_serv.py

then open another shell and shoot msgs at it using:
python zmq_test.py
"""

import zmq
import socket
from pythongrid import zloads

context = zmq.Context()
zsocket = context.socket(zmq.REP)

port = 5002
host_name = socket.gethostname()
ip_address = socket.gethostbyname(host_name)
interface = "tcp://%s" % (ip_address)
home_address = "%s:%i" % (interface, port)

zsocket.bind(home_address)


print "server is running on:", home_address

while True:
    msg_str = zsocket.recv()
    msg = zloads(msg_str)
    print "Got msg:", msg
    zsocket.send("yehaa")

