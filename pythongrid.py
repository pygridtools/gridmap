#! /usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2009 Christian Widmer
# Written (W) 2008-2009 Cheng Soon Ong
# Copyright (C) 2008-2009 Max-Planck-Society


"""
pythongrid provides a high level front end to DRMAA-python.
This module provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.
"""

#paths on cluster file system
#PYTHONPATH = ["/fml/ag-raetsch/home/raetsch/svn/tools/python/", "/fml/ag-raetsch/home/raetsch/svn/tools/python/pythongrid/", '/fml/ag-raetsch/home/raetsch/mylibs/lib/python2.5/site-packages/', '/fml/ag-raetsch/home/raetsch/projects/Git-QPalma/ParaParser', '/fml/ag-raetsch/home/raetsch/projects/Git-QPalma/DynProg/', '/fml/ag-raetsch/share/software/mosek/5/tools/platform/linux64x86/bin', '/fml/ag-raetsch/home/fabio/site-packages', '/fml/ag-raetsch/home/raetsch/projects/Git-QPalma/Genefinding', '/fml/ag-raetsch/share/software/lib/python2.5/site-packages']

# location of pythongrid.py on cluster file system
# TODO set this in configuration file
PYGRID = "/fml/ag-raetsch/home/raetsch/svn/tools/python/pythongrid/pythongrid.py"

# define temp directories for the input and output variables
# (must be writable from cluster)
# TODO define separate client/server TEMPDIR
# ag-raetsch
TEMPDIR = "/fml/ag-raetsch/home/raetsch/tmp/pythongrid"

# agbs
#TEMPDIR = "/agbs/cluster/ong/DRMAA_JOB_OUT"

# used for generating random filenames
alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'


import sys
import os
import os.path
import bz2
import cPickle
import getopt
import time
import random
import traceback

PPATH=reduce(lambda x,y: x+':'+y, PYTHONPATH) ;
print PPATH
os.environ['PYTHONPATH']= PPATH;

sys.path.extend(PYTHONPATH)

print "sys.path=" + str(sys.path) ;

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
    Central entity that wraps a function and its data. Basically,
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
        @param cleanup: flag that determines the cleanup of input and log file
        @type cleanup: boolean
        """
        self.f = f
        self.args = args
        self.kwlist = kwlist
        self.cleanup = cleanup
        self.ret = None
        self.inputfile = ""
        self.outputfile = ""
        self.nativeSpecification = ""
        self.exception = None
        self.environment = None
        self.replace_env = False

        outdir = os.path.expanduser(TEMPDIR)
        if not os.path.isdir(outdir):
            print '%s does not exist. Please create a directory' % outdir
            raise Exception()

        # TODO: ensure uniqueness of file names
        self.name = 'pg'+''.join([random.choice(alphabet) for a in xrange(8)])
        self.inputfile = jp(outdir,self.name + "_in.bz2")
        self.outputfile = jp(outdir,self.name + "_out.bz2")
        self.jobid = ""


    def __repr_broken__(self):
        #TODO: fix representation
        retstr1 = ('%s\nargs=%s\nkwlist=%s\nret=%s\ncleanup=%s' %
                   self.f, self.args, self.kwlist, self.ret, self.cleanup)
        retstr2 = ('\nnativeSpecification=%s\ninputfile=%s\noutputfile=%s\n' %
                   self.nativeSpecification, self.inputfile, self.outputfile)
        return retstr1 + retstr2

    def execute(self):
        """
        Executes function f with given arguments
        and writes return value to field ret.
        If an exception is encountered during execution, ret will
        remain empty and the exception will be written
        to the exception field of the Job object.
        Input data is removed after execution to save space.
        """
        try:
            self.ret = apply(self.f, self.args, self.kwlist)

        except Exception, e:

            print "exception encountered"
            print "type:", str(type(e))
            print "line number:", sys.exc_info()[2].tb_lineno
            print e
            traceback.print_exc(file=sys.stdout)
            print "========="
            self.exception = e


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


    def getNativeSpecification(self):
        """
        define python-style getter
        """

        ret = ""

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

        self.__nativeSpecification = x

    nativeSpecification = property(getNativeSpecification,
                                   setNativeSpecification)



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


def _execute(job):
    """Cannot pickle method instances, so fake a function.
    Used by _process_jobs_locally"""
    
    return apply(job.f, job.args, job.kwlist)



def _process_jobs_locally(jobs, maxNumThreads=None):
    """
    Local execution using the package multiprocessing, if present
    
    @param jobs: jobs to be executed
    @type jobs: list<Job>
    @param maxNumThreads: maximal number of threads
    @type maxNumThreads: int
    
    @return: list of jobs, each with return in job.ret
    @rtype: list<Job>
    """
    
    
    if (not multiprocessing_present or maxNumThreads == 1):
        #perform sequential computation
        for job in jobs:
            job.execute()
    else:
        #print multiprocessing.cpu_count()
        po = multiprocessing.Pool(maxNumThreads)
        result = po.map(_execute, jobs)
        for ix,job in enumerate(jobs):
            job.ret = result[ix]
            
    return jobs


def submit_jobs(jobs):
    """
    Method used to send a list of jobs onto the cluster.
    @param jobs: list of jobs to be executed
    @type jobs: list<Job>
    """

    s = DRMAA.Session()
    s.init()
    jobids = []

    for job in jobs:
        save(job.inputfile, job)
        jt = s.createJobTemplate()

        #fetch only specific env vars from shell
        #shell_env = {"LD_LIBRARY_PATH": os.getenv("LD_LIBRARY_PATH"),
        #             "PYTHONPATH": os.getenv("PYTHONPATH"),
        #             "MOSEKLM_LICENSE_FILE": os.getenv("MOSEKLM_LICENSE_FILE"),
        #             }

        # fetch env vars from shell        
        shell_env = os.environ

        if job.environment and job.replace_env:
            # only consider defined env vars
            jt.setEnvironment(job.environment)

        elif job.environment and not job.replace_env:
            # replace env var from shell with defined env vars
            env = shell_env
            env.update(job.environment)
            jt.setEnvironment(env)

        else:
            # only consider env vars from shell
            jt.setEnvironment(shell_env)

        jt.remoteCommand = os.path.expanduser(PYGRID)
        jt.args = [job.inputfile]
        jt.joinFiles = True
        jt.setNativeSpecification(job.nativeSpecification)
        jt.outputPath = ":" + os.path.expanduser(TEMPDIR)
        jt.errorPath = ":" + os.path.expanduser(TEMPDIR)

        jobid = s.runJob(jt)

        # set job fields that depend on the jobid assigned by grid engine
        job.jobid = jobid
        log_stdout_fn = (os.path.expanduser(TEMPDIR) + job.name + '.o' + jobid)
        log_stderr_fn = (os.path.expanduser(TEMPDIR) + job.name + '.e' + jobid)

        print 'Your job %s has been submitted with id %s' % (job.name, jobid)
        print "stdout:", log_stdout_fn
        print "stderr:", log_stderr_fn
        print ""

        #display tmp file size
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
        
        log_stdout_fn = (os.path.expanduser(TEMPDIR) + job.name + '.o' + jobids[ix])
        log_stderr_fn = (os.path.expanduser(TEMPDIR) + job.name + '.e' + jobids[ix])
        
        try:
            retJob = load(job.outputfile)
            assert(retJob.name == job.name)
            retJobs.append(retJob)

            #print exceptions
            if retJob.exception != None:
                print str(type(retJob.exception))
                print "Exception encountered in job with log file:"
                print log_stdout_fn
                print retJob.exception

            #remove files
            elif retJob.cleanup:

                print "cleaning up:", job.outputfile
                os.remove(job.outputfile)

                if retJob != None:

                    print "cleaning up:", log_stdout_fn
                    os.remove(log_stdout_fn)

                    print "cleaning up:", log_stderr_fn
                    #os.remove(log_stderr_fn)


        except Exception, detail:
            print "error while unpickling file: " + job.outputfile
            print "this could caused by a problem with the cluster environment, imports or environment variables"
            print "check log files for more information: "
            print "stdout:", log_stdout_fn
            print "stderr:", log_stderr_fn
            
            print detail



    return retJobs


def process_jobs(jobs, local=False, maxNumThreads=1):
    """
    Director function to decide whether to run on the cluster or locally
    """
    
    if (not local and drmaa_present):
        # Use submit_jobs and collect_jobs to run jobs and wait for the results.
        (sid, jobids) = submit_jobs(jobs)
        return collect_jobs(sid, jobids, jobs, wait=True)

    elif (not local and not drmaa_present):
        print 'Warning: import DRMAA failed, computing locally'
        return  _process_jobs_locally(jobs, maxNumThreads=maxNumThreads)

    else:
        return  _process_jobs_locally(jobs, maxNumThreads=maxNumThreads)


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
