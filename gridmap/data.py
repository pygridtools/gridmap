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
"""

from __future__ import absolute_import, print_function, unicode_literals

import bz2
try:
    import cPickle as pickle  # For Python 2.x
except ImportError:
    import pickle
import re
from time import sleep

from gridmap import MAX_TRIES, SLEEP_TIME


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
    @type redis_server: L{StrictRedis}
    @param prefix: The prefix to use for the key for this data.
    @type prefix: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """

    # Pickle the obj
    pickled_data = bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)

    # Insert the pickled data into the database
    redis_server.set('{0}_{1}'.format(prefix, job_num), pickled_data)


def zload_db(redis_server, prefix, job_num):
    """
    Loads bz2-compressed pickled object from a Redis database

    @param redis_server: An open connection to the database
    @type redis_server: L{StrictRedis}
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
