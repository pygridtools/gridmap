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
'''
GridMap provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.

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
:var ERROR_MAIL_SENDER: Sender address to use for error emails.
                        (Default: error@gridmap.py)
:var ERROR_MAIL_RECIPIENT: Recipient address for error emails.
                           (Default: $USER@$HOST, where $USER is the current
                            user's username, and $HOST is the last two sections
                            of the server's fully qualified domain name, or just
                            the host's name if it does not contain periods.)
:var MAX_TIME_BETWEEN_HEARTBEATS: How long should we wait (in seconds) for a
                                  heartbeat before we consider a job dead?
                                  (Default: 45)
:var NUM_RESUBMITS: How many times can a particular job can die, before we give
                    up. (Default: 3)
:var CHECK_FREQUENCY: How many seconds pass before we check on the status of a
                      particular job in seconds. (Default: 15)
:var HEARTBEAT_FREQUENCY: How many seconds pass before jobs on the cluster send
                          back heart beats to the submission host.
                          (Default: 10)
:var WEB_PORT: Port to use for CherryPy server when using web monitor.
               (Default: 8076)
'''

from __future__ import absolute_import, print_function, unicode_literals

from gridmap.conf import (CHECK_FREQUENCY, CREATE_PLOTS, DEFAULT_QUEUE,
                          ERROR_MAIL_RECIPIENT, ERROR_MAIL_SENDER,
                          HEARTBEAT_FREQUENCY, 
                          MAX_TIME_BETWEEN_HEARTBEATS, NUM_RESUBMITS,
                          SEND_ERROR_MAILS, SMTP_SERVER, USE_CHERRYPY,
                          USE_MEM_FREE, WEB_PORT)
from gridmap.job import Job, JobException, process_jobs, grid_map, pg_map
from gridmap.version import __version__, VERSION

# For * imports
__all__ = ['Job', 'JobException', 'process_jobs', 'grid_map', 'pg_map',
           'CHECK_FREQUENCY', 'CREATE_PLOTS', 'DEFAULT_QUEUE',
           'ERROR_MAIL_RECIPIENT', 'ERROR_MAIL_SENDER', 'HEARTBEAT_FREQUENCY',
           'MAX_TIME_BETWEEN_HEARTBEATS', 'NUM_RESUBMITS',
           'SEND_ERROR_MAILS', 'SMTP_SERVER', 'USE_CHERRYPY', 'USE_MEM_FREE',
           'WEB_PORT']
