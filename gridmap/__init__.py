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
'''
Grid Map provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.

@author: Christian Widmer
@author: Cheng Soon Ong
@author: Dan Blanchard (dblanchard@ets.org)

@var REDIS_DB: The index of the database to select on the Redis server; can be
               overriden by setting the GRID_MAP_REDIS_DB environment variable.
@var REDIS_PORT: The port of the Redis server to use; can be overriden by
                 setting the GRID_MAP_REDIS_PORT environment variable.
@var USE_MEM_FREE: Does your cluster support specifying how much memory a job
                   will use via mem_free? Can be overriden by setting the
                   GRID_MAP_USE_MEM_FREE environment variable.
@var DEFAULT_QUEUE: The default job scheduling queue to use; can be overriden
                    via the GRID_MAP_DEFAULT_QUEUE environment variable.
@var MAX_TRIES: Maximum number of times to try to get the output of a job from
                the Redis database before giving up and assuming the job died
                before writing its output; can be overriden by setting the
                GRID_MAP_MAX_TRIES environment variable.
@var SLEEP_TIME: Number of seconds to sleep between attempts to retrieve job
                 output from the Redis database; can be overriden by setting the
                 GRID_MAP_SLEEP_TIME environment variable.
'''

from __future__ import absolute_import, print_function, unicode_literals

from gridmap.job import (Job, process_jobs, grid_map, pg_map, USE_MEM_FREE,
                         DEFAULT_QUEUE, REDIS_PORT, REDIS_DB)
from gridmap.data import MAX_TRIES, SLEEP_TIME


# Version info
__version__ = '0.9.5'
VERSION = tuple(int(x) for x in __version__.split('.'))

# For * imports
__all__ = ['Job', 'process_jobs', 'grid_map', 'pg_map', 'USE_MEM_FREE',
           'DEFAULT_QUEUE', 'REDIS_DB', 'REDIS_PORT', 'MAX_TRIES', 'SLEEP_TIME']
