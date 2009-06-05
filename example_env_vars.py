#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2009 Christian Widmer
# Copyright (C) 2008-2009 Max-Planck-Society

import sys
import getopt
import time
import os

from pythongrid import KybJob, Usage
from pythongrid import process_jobs, submit_jobs, collect_jobs, get_status

#needs to be imported, such that module name can be referred to explicitly
import example_advanced



def runExample():

    inputvec = [["0"], ["1"], ["2"]]


    jobs = []

    for input in inputvec:
        # We need to use the full identifier
        # such that the module name is explicit.
        job = KybJob(example_advanced.show_env, input) 
        jobs.append(job)

        
    jobs[1].environment = {"MYENV1": "defined"}
    jobs[1].replace_env = False

    jobs[2].environment = {"MYENV1": "defined"}
    jobs[2].replace_env = True

    processedFunctionJobs = process_jobs(jobs)

    print "ret fields AFTER execution on cluster"
    for (i, job) in enumerate(processedFunctionJobs):
        print "Job #", i, "- ret: ", str(job.ret)


    return jobs


def show_env(n):
    """
    computes factorial of n
    """

    ret = (os.getenv("MYENV1"), os.getenv("MYENV2"))

    return ret


def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
            runExample()

        except getopt.error, msg:
            raise Usage(msg)

    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"

        return 2


if __name__ == "__main__":
    main()

