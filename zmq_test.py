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

if client and server are a different machine, adjust accordingly (see below)

then open another shell and shoot msgs at it using:
python zmq_test.py
"""

print "hint: make sure to start zmq_serv.py before executing this script!"

import zmq
import socket
from pythongrid import zdumps

context = zmq.Context()
zsocket = context.socket(zmq.REQ)


port = 5002
host_name = socket.gethostname()
ip_address = socket.gethostbyname(host_name)
interface = "tcp://%s" % (ip_address)
home_address = "%s:%i" % (interface, port)


# ADJUST IF SERVER ON DIFFERENT MACHINE
zsocket.connect(home_address)


msg_container = {}
msg_container["job_id"] = 0
msg_container["host_name"] = host_name
msg_container["ip_address"] = ip_address
msg_container["command"] = "test"
msg_container["data"] = None

msg_string = zdumps(msg_container)

zsocket.send(msg_string)
ret_msg = zsocket.recv()
print "server replied:", ret_msg

