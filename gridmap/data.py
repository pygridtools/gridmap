# -*- coding: utf-8 -*-

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2013 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2013 ETS

# This file is part of Grid Map.

# Grid Map is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Grid Map is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Grid Map.  If not, see <http://www.gnu.org/licenses/>.

"""
This modules provides all of the data-related function for gridmap.

@author: Christian Widmer
@author: Cheng Soon Ong
@author: Dan Blanchard (dblanchard@ets.org)

@var MAX_TRIES: Maximum number of times to try to get the output of a job from
                the Redis database before giving up and assuming the job died
                before writing its output; can be overriden by setting the
                GRID_MAP_MAX_TRIES environment variable.
@var SLEEP_TIME: Number of seconds to sleep between attempts to retrieve job
                 output from the Redis database; can be overriden by setting the
                 GRID_MAP_SLEEP_TIME environment variable.
"""

from __future__ import absolute_import, print_function, unicode_literals

import bz2
import os
try:
    import cPickle as pickle  # For Python 2.x
except ImportError:
    import pickle
import re
from time import sleep


#### Global settings ####
MAX_TRIES = int(os.getenv('GRID_MAP_MAX_TRIES', '10'))
SLEEP_TIME = int(os.getenv('GRID_MAP_SLEEP_TIME', '3'))


def clean_path(path):
    '''
    Replace all weird SAN paths with normal paths. This is really
    ETS-specific, but shouldn't harm anyone else.
    '''

    path = re.sub(r'/\.automount/\w+/SAN/NLP/(\w+)-(dynamic|static)',
                  r'/home/nlp-\1/\2', path)
    path = re.sub(r'/\.automount/[^/]+/SAN/Research/HomeResearch',
                  '/home/research', path)
    return path


def zsave_db(obj, redis_server, prefix, job_num):
    """
    Saves an object/function as bz2-compressed pickled data in a Redis database

    @param obj: The object/function to store.
    @type obj: C{object} or C{function}
    @param redis_server: An open connection to the database
    @type redis_server: C{StrictRedis}
    @param prefix: The prefix to use for the key for this data.
    @type prefix: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """

    # Pickle the obj
    pickled_data = bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)

    # Insert the pickled data into the database
    redis_server.set('{0}_{1}'.format(prefix, job_num), pickled_data)


def zpublish_db(obj, redis_server, prefix, job_num):
    """
    Sends a bz2-compressed pickled object back to the parent process that
    spawned all of the grid jobs.

    @param obj: The object/function to store.
    @type obj: C{object} or C{function}
    @param redis_server: An open connection to the database
    @type redis_server: C{StrictRedis}
    @param prefix: The prefix to use for the key for this data.
    @type prefix: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """

    # Pickle the obj
    pickled_data = bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)

    # Publish message
    redis_server.publish('{0}_{1}'.format(prefix, job_num), pickled_data)


def zload_db(redis_server, prefix, job_num):
    """
    Loads bz2-compressed pickled object from a Redis database

    @param redis_server: An open connection to the database
    @type redis_server: C{StrictRedis}
    @param prefix: The prefix to use for the key for this data.
    @type prefix: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """
    attempt = 0
    pickled_data = None
    while pickled_data is None and attempt < MAX_TRIES:
        pickled_data = redis_server.get('{0}_{1}'.format(prefix, job_num))
        attempt += 1
        sleep(SLEEP_TIME)
    return pickle.loads(bz2.decompress(pickled_data))
