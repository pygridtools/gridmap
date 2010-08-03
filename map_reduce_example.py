#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2010 Christian Widmer
# Copyright (C) 2010 Max-Planck-Society

import sys
import os
import getopt

from pythongrid import map, reduce, MapReduce

#needs to be imported, such that module name can be referred to explicitly
import example


def computeFactorial(n):
    """
    computes factorial of n
    """
    ret=1
    for i in xrange(n):
        ret=ret*(i+1)

    return ret


def show_list(my_list):
    """
    displays string version of list
    """

    return str(my_list)


def runExample():
    '''
    execute map_reduce example
    ''' 

    param = {"h_vmem": "1G"}
    input = [[20000],[20000],[20000],[20000],[20000],[20000],[20000],[20000],[20000],[20000]]
    map_reduce = MapReduce(example.computeFactorial, show_list, input, param)

    results = map_reduce.wait(False)

    print results


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

