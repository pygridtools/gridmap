#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2012 Christian Widmer
# Copyright (C) 2008-2012 Max-Planck-Society


from pythongrid import KybJob, process_jobs

import time


def compute_factorial(n):
    """
    computes factorial of n
    """

    time.sleep(5)

    ret = 1
    for i in xrange(n):
        ret=ret*(i+1)

    return ret


def make_jobs():
    """
    creates a list of KybJob objects,
    which carry all information needed
    for a function to be executed on SGE:
    - function object
    - arguments
    - settings
    """

    # set up list of arguments
    inputvec = [[3], [5], [10], [20]]

    # create empty job vector
    jobs=[]

    # create job objects
    for arg in inputvec:

        job = KybJob(compute_factorial, arg) 
        job.h_vmem="1000M"
        
        jobs.append(job)
        

    return jobs


def run_example_local_multithreading():
    """
    run a set of jobs on local machine using several cores
    """

    print "====================================="
    print "======  Local Multithreading  ======="
    print "====================================="
    print ""
    print ""

    print "generating function jobs"

    functionJobs = make_jobs()

    # KybJob object start out with an empty ret field, which is only filled after execution
    print "output ret field in each job before multithreaded computation"
    for (i, job) in enumerate(functionJobs):
        print "Job #", i, "- ret: ", job.ret

    print ""
    print "executing jobs on local machine using 3 threads"

    processedFunctionJobs = process_jobs(functionJobs, local=True, maxNumThreads=3)

    print "ret fields AFTER execution on local machine"
    for (i, job) in enumerate(processedFunctionJobs):
        print "Job #", i, "- ret: ", str(job.ret)[0:10]
 


def run_example_cluster():
    """
    run a set of jobs on cluster
    """
    
    print ""
    print ""
    print "====================================="
    print "========   Submit and Wait   ========"
    print "====================================="
    print ""
    print ""

    functionJobs = make_jobs()

    print "output ret field in each job before sending it onto the cluster"
    for (i, job) in enumerate(functionJobs):
        print "Job #", i, "- ret: ", job.ret

    print ""
    print "sending function jobs to cluster"
    print ""

    processedFunctionJobs = process_jobs(functionJobs)

    print "ret fields AFTER execution on cluster"
    for (i, job) in enumerate(processedFunctionJobs):
        print "Job #", i, "- ret: ", str(job.ret)[0:10]



def main(argv=None):
    """
    main function to set up example
    """

    # first we use local multithreading
    run_example_local_multithreading()

    # next we execute function on the cluster
    run_example_cluster()


if __name__ == "__main__":
    main()

