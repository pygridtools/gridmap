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
'''

from __future__ import absolute_import, print_function, unicode_literals

from .job import Job, process_jobs, grid_map, pg_map

#### Global settings ####
# Redis settings
REDIS_DB = 2
REDIS_PORT = 7272
MAX_TRIES = 50
SLEEP_TIME = 3

# Is mem_free configured properly on the cluster?
USE_MEM_FREE = False

# Which queue should we use by default
DEFAULT_QUEUE = 'all.q'

# Version info
__version__ = '0.9.3'
VERSION = tuple(int(x) for x in __version__.split('.'))

# For * imports
__all__ = ['Job', 'process_jobs', 'grid_map', 'pg_map']
