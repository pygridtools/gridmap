#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2010-2012 Christian Widmer
# Copyright (C) 2010-2012 Max-Planck-Society

import sys
import getopt
from pythongrid import MapReduce


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

    serves as a user-defines reduce function
    """

    return str(my_list)


def runExample():
    '''
    execute map_reduce example
    ''' 
    intermediate_results = pg_map(self.fun_map, self.input_list, self.param, local, max_num_threads)


    param = {"h_vmem": "1G"}
    args = [200 ,200 ,200 ,200 ,200 ,200 ,200 ,200 ,200 ,200]
    map_reduce = MapReduce(computeFactorial, show_list, args, param)

    results = map_reduce.wait(True)

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


