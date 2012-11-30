#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2012 Christian Widmer
# Copyright (C) 2008-2012 Max-Planck-Society
#
# Modified to use new API by Daniel Blanchard, November 2012

"""
In addition to the high level map interface "pg_map", pythongrid also allows one to
easily create a list of jobs (that potentially run different functions) and execute
them on the cluster as well.
"""

from __future__ import print_function, unicode_literals

from pythongrid import Job, process_jobs


def compute_factorial(n):
    """
    computes factorial of n
    """
    ret = 1
    for i in xrange(n):
        ret = ret * (i + 1)
    return ret


def make_jobs():
    """
    creates a list of Job objects,
    which carry all information needed
    for a function to be executed on SGE:
    - function object
    - arguments
    - settings
    """

    # set up list of arguments
    inputvec = [[3], [5], [10], [20]]

    # create empty job vector
    jobs = []

    # create job objects
    for arg in inputvec:
        job = Job(compute_factorial, arg)
        jobs.append(job)

    return jobs


def main():
    """
    run a set of jobs on cluster
    """

    print("")
    print("")
    print("=====================================")
    print("========   Submit and Wait   ========")
    print("=====================================")
    print("")
    print("")

    functionJobs = make_jobs()

    print("output ret field in each job before sending it onto the cluster")
    for (i, job) in enumerate(functionJobs):
        print("Job #", i, "- ret: ", job.ret)

    print("")
    print("sending function jobs to cluster")
    print("")

    job_outputs = process_jobs(functionJobs, temp_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/')

    print("ret fields AFTER execution on cluster")
    for (i, result) in enumerate(job_outputs):
        print("Job {}- ret: {}".format(i, result))


if __name__ == "__main__":
    main()

