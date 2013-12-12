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
Some simple unit tests for GridMap.
"""

from __future__ import division, print_function, unicode_literals

import logging
from time import sleep

import gridmap
from gridmap import Job, process_jobs, grid_map, HEARTBEAT_FREQUENCY

from nose.tools import eq_


# Setup logging
logging.captureWarnings(True)
logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +
                            '%(message)s'), level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.debug('Path to gridmap: %s', gridmap)


def compute_factorial(n, wait_sec, repeated):
    '''
    Little function to compute ``n`` factorial and sleep for ``wait_sec``
    seconds.
    '''
    sleep(wait_sec)
    ret = 1
    for i in range(n):
        if repeated:
            sleep(wait_sec)
        ret = ret * (i + 1)
    return ret


def check_map(wait_sec, repeated):
    inputs = [(1, wait_sec, repeated), (2, wait_sec, repeated),
              (4, wait_sec, repeated), (8, wait_sec, repeated),
              (16, wait_sec, repeated)]
    expected = list(map(compute_factorial, inputs))
    outputs = grid_map(compute_factorial, inputs, quiet=False)
    eq_(expected, outputs)


def test_map():
    for wait_sec, repeated in [(0, False), (HEARTBEAT_FREQUENCY + 1, False),
                               (HEARTBEAT_FREQUENCY // 2, True)]:
        yield check_map, wait_sec, repeated


def make_jobs(inputvec, function):
    '''
    Create job list for ``check_process_jobs``
    '''
    # create empty job vector
    jobs = []

    # create job objects
    for arg in inputvec:
        if not isinstance(arg, list):
            arg = [arg]
        job = Job(function, arg)
        jobs.append(job)

    return jobs


def check_process_jobs(wait_sec, repeated):
    inputs = [(1, wait_sec, repeated), (2, wait_sec, repeated),
              (4, wait_sec, repeated), (8, wait_sec, repeated),
              (16, wait_sec, repeated)]
    expected = list(map(compute_factorial, inputs))
    function_jobs = make_jobs(inputs, compute_factorial)
    outputs = process_jobs(function_jobs, quiet=False)
    eq_(expected, outputs)


def test_process_jobs():
    for wait_sec, repeated in [(0, False), (HEARTBEAT_FREQUENCY + 1, False),
                               (HEARTBEAT_FREQUENCY // 2, True)]:
        yield check_map, wait_sec, repeated
