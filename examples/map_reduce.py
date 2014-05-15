#!/usr/bin/env python

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
gridmap provides a high level map interface "grid_map" that can be used
interchangably with python's built in map command.

This example demonstrates how to use that interface.
"""

from __future__ import print_function, unicode_literals

from datetime import datetime

from gridmap import grid_map


def sleep_walk(secs):
    '''
    Pass the time by adding numbers until the specified number of seconds has
    elapsed. Intended as a replacement for ``time.sleep`` that doesn't leave the
    CPU idle (which will make the job seem like it's stalled).
    '''
    start_time = datetime.now()
    num = 0
    while (datetime.now() - start_time).seconds < secs:
        num = num + 1


def computeFactorial(n):
    """
    computes factorial of n
    """
    sleep_walk(10)
    ret = 1
    for i in range(n):
        ret = ret * (i + 1)
    return ret


def main():
    """
    execute map example
    """

    args = [3, 5, 10, 20]

    intermediate_results = grid_map(computeFactorial, args, quiet=False,
                                    max_processes=4)

    # Just print the items instead of really reducing. We could always sum them.
    print("reducing result")
    for i, ret in enumerate(intermediate_results):
        print("f({0}) = {1}".format(args[i], ret))


if __name__ == "__main__":
    main()


