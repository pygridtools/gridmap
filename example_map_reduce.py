#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2010-2012 Christian Widmer
# Copyright (C) 2010-2012 Max-Planck-Society
#
# Modified to use new API by Daniel Blanchard, November 2012


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


