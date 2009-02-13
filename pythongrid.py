#! /usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008 Christian Widmer
# Written (W) 2008 Cheng Soon Ong
# Copyright (C) 2008 Max-Planck-Society


"""
pythongrid provides a high level front end to DRMAA-python.
This module provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.
"""

#paths on cluster file system
#PYTHONPATH = ["~/svn/tools/python/", "~/svn/tools/python/pythongrid/"]

#location of pythongrid.py on cluster file system
#LD_LIBRARY_PATH = "/usr/local/sge/lib/lx26-amd64/"
PYGRID = "~/svn/tools/python/pythongrid/pythongrid.py"

#define temp directories for the input and output variables
#(must be writable from cluster)
# ag-raetsch
TEMPDIR = "~/tmp/"
#TEMPDIR = "/fml/ag-raetsch/home/fabio/tmp/pygrid/"

# agbs
#TEMPDIR = "/agbs/cluster/ong/DRMAA_JOB_OUT"

# used for generating random filenames
alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

#print "LD_LIBRARY_PATH:", os.getenv("LD_LIBRARY_PATH")
#print "PYTHONPATH:", os.getenv("PYTHONPATH")


import sys
import os
import os.path
import bz2
import cPickle
import getopt
import time
import random

jp = os.path.join

drmaa_present=1
multiprocessing_present=1

try:
    import DRMAA
except ImportError, detail:
    print "Error importing DRMAA. Only local multi-threading supported."
    print "Please check your installation."
    print detail
    drmaa_present=0

try:
    import multiprocessing
except ImportError, detail:
    print "Error importing multiprocessing. Local computing limited to one CPU."
    print "Please install python2.6 or the backport of the multiprocessing package"
    print detail
    multiprocessing_present=0


class Job(object):
    """
    Central entity that wrappes a function and its data. Basically,
    a job consists of a function, its argument list, its
    keyword list and a field "ret" which is filled, when
    the execute method gets called
    """

    def __init__(self, f, args, kwlist={}, cleanup=True):
        """
        constructor of Job

        @param f: a function, which should be executed.
        @type f: function
        @param args: argument list of function f
        @type args: list
        @param kwlist: dictionary of keyword arguments
        @type kwlist: dict
        """
        self.f = f
        self.args = args
        self.kwlist = kwlist
        self.cleanup = cleanup
        self.ret = None
        self.nativeSpecification = ""
        self.inputfile = ""
        self.outputfile = ""

    def __repr__(self):
        retstr1 = ('%s\nargs=%s\nkwlist=%s\nret=%s\ncleanup=%s' %
                   self.f, self.args, self.kwlist, self.ret, self.cleanup)
        retstr2 = ('\nnativeSpecification=%s\ninputfile=%s\noutputfile=%s\n' %
                   self.nativeSpecification, self.inputfile, self.outputfile)
        return retstr1 + retstr2

    def execute(self):
        """
        Executes function f with given arguments
        and writes return value to field ret.
        Input data is removed after execution to save space.
        """
        self.ret=apply(self.f, self.args, self.kwlist)


class KybJob(Job):
    """
    Specialization of generic Job that provides an interface to
    the system at MPI Biol. Cyber. Tuebingen.
    """

    def __init__(self, f, args, kwlist={}, cleanup=True):
        """
        constructor of KybJob
        """
        Job.__init__(self, f, args, kwlist, cleanup)
        self.h_vmem = ""
        self.arch = ""
        self.tmpfree = ""
        self.h_cpu = ""
        self.h_rt = ""
        self.express = ""
        self.matlab = ""
        self.simulink = ""
        self.compiler = ""
        self.imagetb = ""
        self.opttb = ""
        self.stattb = ""
        self.sigtb = ""
        self.cplex = ""
        self.nicetohave = ""
        self.jobid = ""

        outdir = os.path.expanduser(TEMPDIR)
        if not os.path.isdir(outdir):
            print '%s does not exist. Please create a directory' % outdir
            raise Exception()
        # TODO: ensure uniqueness of file names
        self.name = 'pg'+''.join([random.choice(alphabet) for a in xrange(8)])
        self.inputfile = jp(outdir,self.name + "_in.bz2")
        self.outputfile =jp(outdir,self.name + "_out.bz2")

    def getNativeSpecification(self):
        """
        define python-style getter
        """

        ret=""

        if (self.name != ""):
            ret = ret + " -N " + str(self.name)
        if (self.h_vmem != ""):
            ret = ret + " -l " + "h_vmem" + "=" + str(self.h_vmem)
        if (self.arch != ""):
            ret = ret + " -l " + "arch" + "=" + str(self.arch)
        if (self.tmpfree != ""):
            ret = ret + " -l " + "tmpfree" + "=" + str(self.tmpfree)
        if (self.h_cpu != ""):
            ret = ret + " -l " + "h_cpu" + "=" + str(self.h_cpu)
        if (self.h_rt != ""):
            ret = ret + " -l " + "h_rt" + "=" + str(self.h_rt)
        if (self.express != ""):
            ret = ret + " -l " + "express" + "=" + str(self.express)
        if (self.matlab != ""):
            ret = ret + " -l " + "matlab" + "=" + str(self.matlab)
        if (self.simulink != ""):
            ret = ret + " -l " + "simulink" + "=" + str(self.simulink)
        if (self.compiler != ""):
            ret = ret + " -l " + "compiler" + "=" + str(self.compiler)
        if (self.imagetb != ""):
            ret = ret + " -l " + "imagetb" + "=" + str(self.imagetb)
        if (self.opttb != ""):
            ret = ret + " -l " + "opttb" + "=" + str(self.opttb)
        if (self.stattb != ""):
            ret = ret + " -l " + "stattb" + "=" + str(self.stattb)
        if (self.sigtb != ""):
            ret = ret + " -l " + "sigtb" + "=" + str(self.sigtb)
        if (self.cplex != ""):
            ret = ret + " -l " + "cplex" + "=" + str(self.cplex)
        if (self.nicetohave != ""):
            ret = ret + " -l " + "nicetohave" + "=" + str(self.nicetohave)

        return ret

    def setNativeSpecification(self, x):
        """
        define python-style setter
        @param x: nativeSpecification string to be set
        @type x: string
        """

        self.__nativeSpecification=x

    nativeSpecification = property(getNativeSpecification,
                                   setNativeSpecification)


#TODO make read-only
#  def __getattribute__(self, name):
#
#    if (name == "nativeSpecification"):
#
#      ret=""
#
#      for attr in self.__dict__.keys():
#
#        print attr
#
#        value = self.__dict__[attr]
#        if (value!=""):
#          ret=ret + " -l " + attr + "=" + value
#
#      return ret
#
#
#    else:
#
#      return Job.__getattribute__(self, name)
#

#TODO this class will most likely disappear, soon.


class MethodJob:

    #TODO derive this from Job, unify!

    methodName = ""
    obj = None
    args = ()
    kwlist = {}
    ret = None

    def __init__(self, m, args, kwlist={}):
        """

        @param m: method to execute
        @type m: method
        @param args: list of arguments
        @type args: list
        @param kwlist: keyword list
        @type kwlist: dict
        """

        self.methodName = m.im_func.func_name
        self.obj = m.im_self
        self.args = args
        self.kwlist = kwlist

    def execute(self):
        m = getattr(self.obj, self.methodName)
        self.ret = apply(m, self.args, self.kwlist)


#only define this class if the multiprocessing module is present
if multiprocessing_present:

    class JobsProcess(multiprocessing.Process):
        """
        In case jobs are to be computed locally, a number of Jobs (possibly one)
        are assinged to one thread.
        """

        def __init__(self, jobs):
            """
            Constructor
            @param jobs: list of jobs
            @type jobs: list of Job objects
            """

            self.jobs = jobs
            multiprocessing.Process.__init__(self)

        def run(self):
            """
            Executes each job in job list
            """
            for job in self.jobs:
                job.execute()


def _process_jobs_locally(jobs, maxNumThreads=1):
    """
    Run jobs on local machine in a multithreaded manner,
    providing the same interface.

    @param jobs: list of jobs to be executed locally.
    @type jobs: list of Job objects
    @param maxNumThreads: defines the maximal number of threads
                          to be used to process jobs
    @type maxNumThreads: integer
    """


    #perform sequential computation
    if (not multiprocessing_present):
        for job in jobs:
            job.execute()

        return jobs


    #TODO: implement this using the processing package for python2.5 or
    #the multiprocessing library in python2.6, including a queue

    numJobs=len(jobs)

    print "number of jobs: ", numJobs

    #check if there are fewer jobs then allowed threads
    if (maxNumThreads >= numJobs):
        numThreads = numJobs
        jobsPerThread = 1
    else:
        numThreads = maxNumThreads
        jobsPerThread = int(float(numJobs)/float(numThreads)+0.5)

    print "number of threads: ", numThreads
    print "jobs per thread: ", jobsPerThread

    jobList = []
    processList = []

    #assign jobs to process
    #TODO: use a queue here
    for (i, job) in enumerate(jobs):
        jobList.append(job)

        if ((i%jobsPerThread==0 and i!=0) or i==(numJobs-1)):
            #create new process
            print "starting new process"
            process = JobsProcess(jobList)
            processList.append(process)
            process.start()
            jobList = []


    #wait for process to finish
    for process in processList:
        process.join()

    return jobs


def submit_jobs(jobs):
    """
    Method used to send a list of jobs onto the cluster.
    @param jobs: list of jobs to be executed
    @type jobs: list of Job objects
    """

    s = DRMAA.Session()
    s.init()
    jobids = []

    for job in jobs:
        save(job.inputfile, job)
        jt = s.createJobTemplate()

        #TODO figure this out for agbs
        jt.setEnvironment({"LD_LIBRARY_PATH": os.getenv("LD_LIBRARY_PATH"),
                           "PYTHONPATH": os.getenv("PYTHONPATH"),
                           "MOSEKLM_LICENSE_FILE": os.getenv("MOSEKLM_LICENSE_FILE"),
                           })

        jt.remoteCommand = os.path.expanduser(PYGRID)
        jt.args = [job.inputfile]
        jt.joinFiles = True
        jt.setNativeSpecification(job.nativeSpecification)
        jt.outputPath = ":" + os.path.expanduser(TEMPDIR)

        jobid = s.runJob(jt)
        job.jobid = jobid
        print 'Your job %s has been submitted with id %s' % (job.name, jobid)

        #display file size
        # print os.system("du -h " + invars)

        jobids.append(jobid)
        s.deleteJobTemplate(jt)

    sid = s.getContact()
    s.exit()

    return (sid, jobids)


def collect_jobs(sid, jobids, joblist, wait=False):
    """
    Collect the results from the jobids, returns a list of Jobs

    @param sid: session identifier
    @type sid: string returned by cluster
    @param jobids: list of job identifiers returned by the cluster
    @type jobids: list of strings
    @param wait: Wait for jobs to finish?
    @type wait: Boolean, defaults to False
    """
    for ix in xrange(len(jobids)):
        assert(jobids[ix] == joblist[ix].jobid)

    s = DRMAA.Session()
    s.init(sid)

    if wait:
        drmaaWait = DRMAA.Session.TIMEOUT_WAIT_FOREVER
    else:
        drmaaWait = DRMAA.Session.TIMEOUT_NO_WAIT

    s.synchronize(jobids, drmaaWait, True)
    print "success: all jobs finished"
    s.exit()

    #attempt to collect results
    retJobs = []
    for ix, job in enumerate(joblist):
        try:
            retJob = load(job.outputfile)
            assert(retJob.name == job.name)
            retJobs.append(retJob)

            #remove files
            if retJob.cleanup:
                os.remove(job.outputfile)
                logfilename = (os.path.expanduser(TEMPDIR)
                               + job.name + '.o' + jobids[ix])
                print logfilename
                os.remove(logfilename)

        except Exception, detail:
            print "error while unpickling file: " + job.outputfile
            print "most likely there was an error during job execution"

            print detail



    return retJobs


def process_jobs(jobs, local=False, maxNumThreads=1):
    """
    Director method to decide whether to run on cluster or locally
    """
    if (not local and drmaa_present):
        #Use submit_jobs and collect_jobs to run jobs and wait for the results.
        (sid, jobids) = submit_jobs(jobs)
        return collect_jobs(sid, jobids, jobs, wait=True)
    elif (not local and not drmaa_present):
        print 'Warning: import DRMAA failed, computing locally'
        return _process_jobs_locally(jobs, maxNumThreads=maxNumThreads)
    else:
        return _process_jobs_locally(jobs, maxNumThreads=maxNumThreads)


def get_status(sid, jobids):
    """
    Get the status of all jobs in jobids.
    Returns True if all jobs are finished.

    There is some instability in determining job completion
    """
    _decodestatus = {
        -42: 'sge and drmaa not in sync',
        DRMAA.Session.UNDETERMINED: 'process status cannot be determined',
        DRMAA.Session.QUEUED_ACTIVE: 'job is queued and active',
        DRMAA.Session.SYSTEM_ON_HOLD: 'job is queued and in system hold',
        DRMAA.Session.USER_ON_HOLD: 'job is queued and in user hold',
        DRMAA.Session.USER_SYSTEM_ON_HOLD: 'job is in user and system hold',
        DRMAA.Session.RUNNING: 'job is running',
        DRMAA.Session.SYSTEM_SUSPENDED: 'job is system suspended',
        DRMAA.Session.USER_SUSPENDED: 'job is user suspended',
        DRMAA.Session.DONE: 'job finished normally',
        DRMAA.Session.FAILED: 'job finished, but failed',
        }

    s = DRMAA.Session()
    s.init(sid)
    status_summary = {}.fromkeys(_decodestatus, 0)
    for jobid in jobids:
        try:
            curstat = s.getJobProgramStatus(jobid)
        except DRMAA.InvalidJobError, message:
            print message
            status_summary[-42] += 1
        else:
            status_summary[curstat] += 1

    print 'Status of %s at %s' % (sid, time.strftime('%d/%m/%Y - %H.%M:%S'))
    for curkey in status_summary.keys():
        if status_summary[curkey]>0:
            print '%s: %d' % (_decodestatus[curkey], status_summary[curkey])
    s.exit()

    return ((status_summary[DRMAA.Session.DONE]
             +status_summary[-42])==len(jobids))


#####################################################################
# Dealing with data
#####################################################################


def save(filename, myobj):
    """
    Save myobj to filename using pickle
    """
    try:
        f = bz2.BZ2File(filename, 'wb')
    except IOError, details:
        sys.stderr.write('File ' + filename + ' cannot be written\n')
        sys.stderr.write(details)
        return

    cPickle.dump(myobj, f, protocol=2)
    f.close()


def load(filename):
    """
    Load from filename using pickle
    """
    try:
        f = bz2.BZ2File(filename, 'rb')
    except IOError, details:
        sys.stderr.write('File ' + filename + ' cannot be read\n')
        sys.stderr.write(details)
        return

    myobj = cPickle.load(f)
    f.close()
    return myobj


################################################################
#      The following code will be executed on the cluster      #
################################################################


def run_job(pickleFileName):
    """
    This is the code that is executed on the cluster side.
    Runs job which was pickled to a file called pickledFileName.

    @param pickleFileName: filename of pickled Job object
    @type pickleFileName: string
    """

    inPath = pickleFileName
    job = load(inPath)

    job.execute()

    #remove input file
    if job.cleanup:
        os.remove(job.inputfile)

    save(job.outputfile, job)


class Usage(Exception):
    """
    Simple Exception for cmd-line user-interface.
    """

    def __init__(self, msg):
        """
        Constructor of simple Exception.

        @param msg: exception message
        @type msg: string
        """

        self.msg = msg


def main(argv=None):
    """
    Parse the command line inputs and call run_job

    @param argv: list of arguments
    @type argv: list of strings
    """


    if argv is None:
        argv = sys.argv

    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
            run_job(args[0])
        except getopt.error, msg:
            raise Usage(msg)


    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"

        return 2

if __name__ == "__main__":
    main(sys.argv)
