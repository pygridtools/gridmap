#!/usr/bin/env python

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2013 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2013 Educational Testing Service

# This file is part of Python Grid.

# Python Grid is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Python Grid is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Python Grid.  If not, see <http://www.gnu.org/licenses/>.


"""
pythongrid provides a high level map interface "pg_map" that can
be used interchangably with python's built in map command.

This example demonstrates how to use that interface.
"""

from __future__ import print_function, unicode_literals

from pythongrid import pg_map


def computeFactorial(n):
    """
    computes factorial of n
    """
    ret = 1
    for i in xrange(n):
        ret = ret * (i + 1)
    return ret


def main():
    """
    execute map example
    """

    args = [1, 2, 4, 8, 16]

    intermediate_results = pg_map(computeFactorial, args, quiet=False)

    print("reducing result")
    for i, ret in enumerate(intermediate_results):
        print("f({}) = {}".format(args[i], ret))


if __name__ == "__main__":
    main()


