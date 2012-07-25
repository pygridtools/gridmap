#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2010-2012 Christian Widmer
# Copyright (C) 2010-2012 Max-Planck-Society

"""
pythongrid provides a high level map interface "pg_map" that can
be used interchangably with python's built in map command.

This example demonstrates how to use that interface.
"""

import sys
import getopt
from pythongrid import pg_map


def computeFactorial(n):
    """
    computes factorial of n
    """
    ret=1
    for i in xrange(n):
        ret=ret*(i+1)

    return ret


def runExample():
    """
    execute map example
    """

    args = [1, 2, 4, 8, 16]
    local = True
    max_num_threads = 3
    param = {"h_vmem": "1G"}

    intermediate_results = pg_map(computeFactorial, args, param, local, max_num_threads)

    print "reducing result"
    for i, ret in enumerate(intermediate_results):
        print "f(%i) = %i" % (args[i], ret)


def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "h", ["help"])
        runExample()

    except getopt.error, msg:
        print msg



if __name__ == "__main__":
    main()


