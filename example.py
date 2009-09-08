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
import pythongrid
from pythongrid import KybJob, Usage
from pythongrid import process_jobs, submit_jobs, collect_jobs, get_status
import time

#needs to be imported, such that module name can be referred to explicitly
import example


def makeJobs():
    """
    Creates a list of Jobs.
    """
    
    inputvec = [[3], [5], [10], [15]]
    print 'print computing the factorials of %s' % str(inputvec)
    jobs=[]

    for input in inputvec:
        # We need to use the full identifier
        # such that the module name is explicit.
        job = KybJob(example.computeFactorial, input) 
        job.h_vmem="300M"
        
        jobs.append(job)
        

    return jobs


def runExample():
    '''
    execute example    
    ''' 

    print "====================================="
    print "======  Local Multithreading  ======="
    print "====================================="
    print ""
    print ""

    print "generating function jobs"

    functionJobs = makeJobs()

    print "output ret field in each job before multithreaded computation"
    for (i, job) in enumerate(functionJobs):
        print "Job #", i, "- ret: ", job.ret

    print ""
    print "executing jobs on local machine using 3 threads"
    if not pythongrid.multiprocessing_present:
        print 'multiprocessing not found, serial computation'
    print ""


    processedFunctionJobs = process_jobs(functionJobs, local=True, maxNumThreads=3)
    

    print "ret fields AFTER execution on local machine"
    for (i, job) in enumerate(processedFunctionJobs):
        print "Job #", i, "- ret: ", str(job.ret)[0:10]
    
    
    print ""
    print ""
    print "====================================="
    print "========   Submit and Wait   ========"
    print "====================================="
    print ""
    print ""

    functionJobs = makeJobs()

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


    print ""
    print ""
    print "====================================="
    print "=======  Submit and Forget   ========"
    print "====================================="
    print ""
    print ""


    print 'demo session'
    myjobs = makeJobs()

    (sid, jobids) = submit_jobs(myjobs)

    print 'checking whether finished'
    while not get_status(sid, jobids):
        time.sleep(7)
    print 'collecting jobs'
    retjobs = collect_jobs(sid, jobids, myjobs)
    print "ret fields AFTER execution on cluster"
    for (i, job) in enumerate(retjobs):
        print "Job #", i, "- ret: ", str(job.ret)[0:10]

    print '--------------'


def computeFactorial(n):
    """
    computes factorial of n
    """
    ret=1
    for i in xrange(n):
        ret=ret*(i+1)

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

