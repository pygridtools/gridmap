# -*- coding: utf-8 -*-

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2013 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2013 ETS

# This file is part of GridMap.

# GridMap is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# GridMap is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with GridMap.  If not, see <http://www.gnu.org/licenses/>.
"""
Global settings for GridMap. All of these settings can be overridden by
specifying environment variables with the same name.

:author: Christian Widmer
:author: Cheng Soon Ong
:author: Dan Blanchard (dblanchard@ets.org)

:var USE_MEM_FREE: Does your cluster support specifying how much memory a job
                   will use via mem_free? (Default: ``False``)
:var DEFAULT_QUEUE: The default job scheduling queue to use.
                    (Default: ``all.q``)
:var CREATE_PLOTS: Should we plot cpu and mem usage and send via email?
                   (Default: ``True``)
:var USE_CHERRYPY: Should we start web monitoring interface?
                   (Default: ``True``)
:var SEND_ERROR_MAILS: Should we send error emails?
                       (Default: ``False``)
:var SMTP_SERVER: SMTP server for sending error emails.
                  (Default: last three sections of the current machine's fully
                  qualified domain name)
:var ERROR_MAIL_SENDER: Sender address to use for error emails.
                        (Default: error@gridmap.py)
:var ERROR_MAIL_RECIPIENT: Recipient address for error emails.
                           (Default: $USER@$HOST, where $USER is the current
                           user's username, and $HOST is the last two sections
                           of the current machine's fully qualified domain name,
                           or just the hostname if it does not contain periods.)
:var MAX_TIME_BETWEEN_HEARTBEATS: How long should we wait (in seconds) for a
                                  heartbeat before we consider a job dead?
                                  (Default: 90)
:var IDLE_THRESHOLD: Percent CPU utilization (ratio of CPU time to real time
                     * 100) that a process must drop below to be considered not
                     running.
                     (Default: 1.0)
:var MAX_IDLE_HEARTBEATS: Number of heartbeats we can receive where the process
                          has >= IDLE_THRESHOLD CPU utilization and is sleeping
                          before we consider the process dead. (Default: 3)
:var NUM_RESUBMITS: How many times can a particular job can die, before we give
                    up. (Default: 3)
:var CHECK_FREQUENCY: How many seconds pass before we check on the status of a
                      particular job in seconds. (Default: 15)
:var HEARTBEAT_FREQUENCY: How many seconds pass before jobs on the cluster send
                          back heart beats to the submission host.
                          (Default: 10)
:var WEB_PORT: Port to use for CherryPy server when using web monitor.
               (Default: 8076)

"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
import os
from socket import gethostname


# Check if certain libraries are present
try:
    import drmaa
    DRMAA_PRESENT = True
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning('Could not import drmaa. Only local multiprocessing ' +
                   'supported.')
    DRMAA_PRESENT = False

# plot cpu and mem usage and send via email
CREATE_PLOTS = 'TRUE' == os.getenv('CREATE_PLOTS', 'True').upper()
if CREATE_PLOTS:
    try:
        import matplotlib
    except ImportError:
        logger = logging.getLogger(__name__)
        logger.warning('Could not import matplotlib. No plots will be created' +
                       ' in debug emails.')
        CREATE_PLOTS = False

# enable web-interface to monitor jobs
USE_CHERRYPY = 'TRUE' == os.getenv('USE_CHERRYPY', 'True').upper()
if USE_CHERRYPY:
    try:
        import cherrypy
    except ImportError:
        logger = logging.getLogger(__name__)
        logger.warning('Could not import cherrypy. Web-based monitoring will ' +
                       'be disabled.')
        USE_CHERRYPY = False

# Global settings ####
# email settings
SEND_ERROR_MAILS = 'TRUE' == os.getenv('SEND_ERROR_MAILS', 'True').upper()
SMTP_SERVER = os.getenv('SMTP_SERVER', '.'.join(gethostname().split('.')[-3:]))
ERROR_MAIL_SENDER = os.getenv('ERROR_MAIL_SENDER', 'error@gridmap.py')
ERROR_MAIL_RECIPIENT = os.getenv('ERROR_MAIL_RECIPIENT',
                                 '{}@{}'.format(os.getenv('USER'),
                                                '.'.join(gethostname().split('.')[-2:])))

# how much time can pass between heartbeats, before
# job is assummed to be dead in seconds
MAX_TIME_BETWEEN_HEARTBEATS = int(os.getenv('MAX_TIME_BETWEEN_HEARTBEATS',
                                            '90'))

IDLE_THRESHOLD = float(os.getenv('IDLE_THRESHOLD', '1.0'))
MAX_IDLE_HEARTBEATS = int(os.getenv('MAX_IDLE_HEARTBEATS', '3'))

# defines how many times can a particular job can die,
# before we give up
NUM_RESUBMITS = int(os.getenv('NUM_RESUBMITS', '3'))

# check interval: how many seconds pass before we check
# on the status of a particular job in seconds
CHECK_FREQUENCY = int(os.getenv('CHECK_FREQUENCY', '15'))

# heartbeat frequency: how many seconds pass before jobs
# on the cluster send back heart beats to the submission
# host
HEARTBEAT_FREQUENCY = int(os.getenv('HEARTBEAT_FREQUENCY', '10'))

# Is mem_free configured properly on the cluster?
USE_MEM_FREE = 'TRUE' == os.getenv('USE_MEM_FREE', 'False').upper()

# Which queue should we use by default
DEFAULT_QUEUE = os.getenv('DEFAULT_QUEUE', 'all.q')

# Port to use for web server
WEB_PORT = 8076
