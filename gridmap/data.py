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
This modules provides all of the data-related function for gridmap.

:author: Christian Widmer
:author: Cheng Soon Ong
:author: Dan Blanchard (dblanchard@ets.org)

"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import bz2
try:
    import cPickle as pickle  # For Python 2.x
except ImportError:
    import pickle
import re


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


def zdumps(obj):
    """
    dumps pickleable object into bz2 compressed string

    :param obj: The object/function to store.
    :type obj: object or function

    :returns: An bz2-compressed pickle of the given object.
    """
    return bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)


def zloads(pickled_data):
    """
    loads pickleable object from bz2 compressed string

    :param pickled_data: BZ2 compressed byte sequence
    :type pickled_data: bytes

    :returns: An unpickled version of the compressed byte sequence.
    """
    return pickle.loads(bz2.decompress(pickled_data))

