#! /usr/bin/env python
# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2011 Christian Widmer
# Written (W) 2008-2011 Cheng Soon Ong
# Copyright (C) 2008-2011 Max-Planck-Society

"""
pythongrid provides a high level front-end to DRMAA-python.
This module provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.
"""

import sys
import os
import os.path
import subprocess
import gzip
import cPickle
import getopt
import time
import random
import traceback
import zmq
import socket
import zlib
import string
import uuid   
import email
import smtplib
import cStringIO
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


from datetime import datetime

#import default configuration
from pythongrid_cfg import *

##CFG structure loaded 

#paths on cluster file system
# TODo set this in configuration file

# location of pythongrid.py on cluster file system
# ToDO set this in configuration file


# define temp directories for the input and output variables
# (must be writable from cluster)
# ToDO define separate client/server CFG['TEMPDIR']



#PPATH = reduce(lambda x,y: x+':'+y, PYTHONPATH)
#print PPATH
#os.environ['PYTHONPATH'] = PPATH
#sys.path.extend(PYTHONPATH)


jp = os.path.join

DRMAA_PRESENT = True
MULTIPROCESSING_PRESENT = True
MATPLOTLIB_PRESENT = True
CHERRYPY_PRESENT = True

try:
    import drmaa
except Exception, detail:
    print "Error importing drmaa. Only local multi-threading supported."
    print "Please check your installation."
    print detail
    DRMAA_PRESENT = False

try:
    import multiprocessing
except Exception, detail:
    print "Error importing multiprocessing. Local computing limited to one CPU."
    print "Please install python2.6 or the backport of the multiprocessing package"
    print detail
    MULTIPROCESSING_PRESENT = False

try:
    import pylab
except Exception, detail:
    #print "Error importing pylab. Not plots will be created in debugging emails."
    #print "Please check your installation."
    #print detail
    MATPLOTLIB_PRESENT = False

try:
    import cherrypy
except Exception, detail:
    print "Error importing cherrypy. Web-based monitoring will be disabled."
    print "Please check your installation."
    print detail
    CHERRYPY_PRESENT = False




class Job(object):
    """
    Central entity that wraps a function and its data. Basically,
    a job consists of a function, its argument list, its
    keyword list and a field "ret" which is filled, when
    the execute method gets called
    """

    def __init__(self, f, args, kwlist={}, param=None, cleanup=True):
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
        self.nativeSpecification = ""
        self.exception = None
        self.environment = None
        self.replace_env = False
        self.working_dir = None

        if param!=None:
            self.__set_parameters(param)

        outdir = os.path.expanduser(CFG['TEMPDIR'])
        if not os.path.isdir(outdir):
            print '%s does not exist. Please create a directory' % outdir
            raise Exception()

        self.name = 'pg_' + str(uuid.uuid1())
        self.jobid = ""


    def __set_parameters(self, param):
        """
        method to set parameters from dict
        """

        assert(param!=None)

        for (key, value) in param.items():
            setattr(self, key, value)

        return self


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
            print "========="
            self.exception = traceback.format_exc()
            self.ret = e

            print self.exception
            traceback.print_exc(file=sys.stdout)


class KybJob(Job):
    """
    Specialization of generic Job that provides an interface to
    the system at MPI Biol. Cyber. Tuebingen.
    """

    def __init__(self, f, args, kwlist={}, param=None, cleanup=True):
        """
        constructor of KybJob
        """
        Job.__init__(self, f, args, kwlist, param=param, cleanup=cleanup)
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

        # additional fields for robustness
        self.num_resubmits = 0
        self.cause_of_death = ""
        self.jobid = -1
        self.host_name = ""
        self.timestamp = None
        self.heart_beat = None
        self.exception = None
        
        # fields to track mem/cpu-usage
        self.track_mem = []
        self.track_cpu = []

        # black and white lists
        self.white_list = ""
        #working directory
        self.working_dir =  os.getcwd()


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

        if (self.white_list != ""):
            ret = ret + " -q "
            for i in range(len(self.white_list)-1):
                ret = ret + self.white_list[i] + ","
            ret = ret + self.white_list[-1]

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

    
    def is_out_of_memory(self):
        """
        checks if job is out of memory
        according to requested resources 
        and the last heart-beat
        """

        # compare memory consumption from
        # last heart-beat to requested memory
        if self.heart_beat != None:

            unit = self.h_vmem[-1]
            allocated_mb = float(self.h_vmem[0:-1])

            if (unit == "G"):
                allocated_mb = allocated_mb * 1000

            # 10% more mem, at least 1G more than last heart-beat
            upper_bound_mem = max(float(self.heart_beat["memory"]) + 1000, float(self.heart_beat["memory"])*1.10)
            
            # if we are within memory limit
            if upper_bound_mem  > allocated_mb:
                return True

        # if we have a memory exception
        if isinstance(self.ret, MemoryError): 
            return True
        
        # Sometimes out-of-memory causes a System Error
        if isinstance(self.ret, SystemError):

            # we look for memory keyword in exception detail (heuristic but useful)
            exception_detail = str(self.exception).lower()

            if exception_detail.find("memory") != -1:
                return True

        # other problem
        return False
        

    def alter_allocated_memory(self, factor=2.0):
        """
        increases requested memory by a certain factor
        """
        
        assert(type(factor)==float)

        if self.h_vmem != "":
            unit = self.h_vmem[-1]
            allocated_mb = float(self.h_vmem[0:-1])
            self.h_vmem = "%f%s" % (factor*allocated_mb, unit)

            print "memory for job %s increased to %s" % (self.name, self.h_vmem)

        return self



#only define this class if the multiprocessing module is present
if MULTIPROCESSING_PRESENT:

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



def _process_jobs_locally(jobs, maxNumThreads=1):
    """
    Local execution using the package multiprocessing, if present
    
    @param jobs: jobs to be executed
    @type jobs: list<Job>
    @param maxNumThreads: maximal number of threads
    @type maxNumThreads: int
    
    @return: list of jobs, each with return in job.ret
    @rtype: list<Job>
    """
   
    print "using %i threads" % (maxNumThreads)
    
    if (not MULTIPROCESSING_PRESENT or maxNumThreads == 1):
        #perform sequential computation
        for job in jobs:
            job.execute()
    else:
        po = multiprocessing.Pool(maxNumThreads)
        result = po.map(_execute, jobs)
        for ix,job in enumerate(jobs):
            job.ret = result[ix]
            
    return jobs


def submit_jobs(jobs, home_address, white_list=""):
    """
    Method used to send a list of jobs onto the cluster.
    @param jobs: list of jobs to be executed
    @type jobs: list<Job>
    """

    session = drmaa.Session()
    session.initialize()
    jobids = []

    for job in jobs:
        # set job white list
        job.white_list = white_list

        # remember address of submission host
        job.home_address = home_address

        # append jobs
        jobid = append_job_to_session(session, job)
        jobids.append(jobid)

    sid = session.contact
    session.exit()

    return (sid, jobids)



def append_job_to_session(session, job):
    """
    For an active session, append new job
    based on information stored in job object

    side-effects:
    - job.jobid set to jobid determined by grid-engine
    - job.log_stdout_fn set to std::out file
    - job.log_stderr_fn set to std::err file
    """

    jt = session.createJobTemplate()

    #TODO: fix handling of environment variables
    #fetch only specific env vars from shell
    #shell_env = {"LD_LIBRARY_PATH": os.getenv("LD_LIBRARY_PATH"),
    #             "PYTHONPATH": os.getenv("PYTHONPATH"),
    #             "MOSEKLM_LICENSE_FILE": os.getenv("MOSEKLM_LICENSE_FILE"),
    #             }

    # fetch env vars from shell        
    shell_env = os.environ

    if job.environment and job.replace_env:
        # only consider defined env vars
        jt.jobEnvironment = job.environment

    elif job.environment and not job.replace_env:
        # replace env var from shell with defined env vars
        env = shell_env
        env.update(job.environment)
        jt.jobEnvironment = env

    else:
        # only consider env vars from shell
        jt.jobEnvironment = shell_env
        

    jt.remoteCommand = os.path.expanduser(CFG['PYGRID'])
    jt.args = [job.name, job.home_address]
    jt.joinFiles = True
    jt.nativeSpecification = job.nativeSpecification
    jt.outputPath = ":" + os.path.expanduser(CFG['TEMPDIR'])
    jt.errorPath = ":" + os.path.expanduser(CFG['TEMPDIR'])

    jobid = session.runJob(jt)

    # set job fields that depend on the jobid assigned by grid engine
    job.jobid = jobid
    job.log_stdout_fn = (os.path.expanduser(CFG['TEMPDIR']) + job.name + '.o' + jobid)
    job.log_stderr_fn = (os.path.expanduser(CFG['TEMPDIR']) + job.name + '.e' + jobid)

    print 'Your job %s has been submitted with id %s' % (job.name, jobid)
    print "stdout:", job.log_stdout_fn
    print "stderr:", job.log_stderr_fn
    print ""

    session.deleteJobTemplate(jt)

    return jobid


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

    s = drmaa.Session()
    s.initialize(sid)

    if wait:
        drmaaWait = drmaa.Session.TIMEOUT_WAIT_FOREVER
    else:
        drmaaWait = drmaa.Session.TIMEOUT_NO_WAIT

    s.synchronize(jobids, drmaaWait, True)
    print "success: all jobs finished"
    s.exit()

    #attempt to collect results
    retJobs = []
    for ix, job in enumerate(joblist):
        
        log_stdout_fn = (os.path.expanduser(CFG['TEMPDIR']) + job.name + '.o' + jobids[ix])
        log_stderr_fn = (os.path.expanduser(CFG['TEMPDIR']) + job.name + '.e' + jobids[ix])
        
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
    local: local or cluster processing
    """
    
    if (not local and DRMAA_PRESENT):

        # get list of trusted nodes
        white_list = get_white_list()

        # initialize checker to get port number
        checker = StatusCheckerZMQ()

        # get interface and port
        home_address = checker.home_address

        # jobid field is attached to each job object
        (sid, jobids) = submit_jobs(jobs, home_address, white_list)

        # handling of inputs, outputs and heartbeats
        checker.check(sid, jobs)

        return jobs

    elif (not local and not DRMAA_PRESENT):
        print 'Warning: import drmaa failed, computing locally'
        return  _process_jobs_locally(jobs, maxNumThreads=maxNumThreads)

    else:
        return  _process_jobs_locally(jobs, maxNumThreads=maxNumThreads)



def get_status(sid, jobids):
    """
    Get the status of all jobs in jobids.
    Returns True if all jobs are finished.

    Using the state-aware StatusChecker now, this
    function maintains the previous
    interface.
    """

    checker = StatusChecker(sid, jobids)
    return checker.check()



class StatusChecker(object):
    """
    To deal with the fact that grid engine seems
    to forget about finished jobs after a little while,
    we need to keep track of finished jobs manually
    and count a out-of-synch job as finished, if it has
    previously had the status "finished"
    """ 

    def __init__(self, sid, jobs):
        """
        we keep a memory of job ids
        """
        
        self.jobs = jobs
        self.jobids = [job.jobid for job in jobs]
        self.sid = sid
        self.jobid_to_status = {}.fromkeys(self.jobids, 0)

        self._decodestatus = {
            -42: 'sge and drmaa not in sync',
            "undetermined": 'process status cannot be determined',
            "queued_active": 'job is queued and active',
            "system_on_hold": 'job is queued and in system hold',
            "user_on_hold": 'job is queued and in user hold',
            "user_system_on_hold": 'job is in user and system hold',
            "running": 'job is running',
            "system_suspended": 'job is system suspended',
            "user_suspended": 'job is user suspended',
            "done": 'job finished normally',
            "failed": 'job finished, but failed',
        }
            

    def check(self):
        """
        Get the status of all jobs.
        Returns True if all jobs are finished.
        """
                
        s = drmaa.Session()
        s.initialize(self.sid)

        status_summary = {}.fromkeys(self._decodestatus, 0)
        status_changed = False

        for job in self.jobs:

            jobid = job.jobid
            old_status = self.jobid_to_status[jobid]
            
            try:
                curstat = s.jobStatus(jobid)

            except Exception, message:
                # handle case where already finished job
                # is now out-of-synch with grid engine
                if old_status == "done":
                    curstat = "done"
                else:
                    curstat = -42


            # print job status updates
            if curstat != old_status:

                # set flag
                status_changed = True

                # determine node name
                job.host_name = check_host_name(jobid)

                print "status update for job", jobid, "from", old_status, "to", curstat, "log at", job.log_stdout_fn, "on", job.host_name
    
                # check cause of death and resubmit if unnatural
                if curstat == "done" or curstat == -42:
                    resubmit = handle_resubmit(s, job)


            # remember current status
            self.jobid_to_status[jobid] = curstat
            status_summary[curstat] += 1


        # print status summary
        if status_changed:
            print 'Status of %s at %s' % (self.sid, time.strftime('%d/%m/%Y - %H.%M:%S'))
            for curkey in status_summary.keys():
                if status_summary[curkey]>0:
                    print '%s: %d' % (self._decodestatus[curkey], status_summary[curkey])
            print "##############################################"
            status_changed = False

        s.exit()

        return (status_summary["done"] + status_summary[-42]==len(self.jobs))



class StatusCheckerZMQ(object):
    """
    switched to next-generation cluster computing :D
    """ 

    def __init__(self):
        """
        set up socket
        """

        context = zmq.Context()
        self.socket = context.socket(zmq.REP)

        self.host_name = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.host_name)
        self.interface = "tcp://%s" % (self.ip_address)

        # bind to random port and remember it
        self.port = self.socket.bind_to_random_port(self.interface)
        self.home_address = "%s:%i" % (self.interface, self.port)

        print "setting up connection on", self.home_address

        if False and CHERRYPY_PRESENT:
            print "starting web interface"
            Popen("python pythongrid_web.py " + self.home_address)


        # uninitialized field (set in check method)
        self.jobs = []
        self.jobids = []
        self.session_id = -1
        self.jobid_to_job = {}


    def __del__(self):
        """
        clean up open socket
        """

        self.socket.close()


    def check(self, session_id, jobs):
        """
        serves input and output data
        """

        # save list of jobs
        self.jobs = jobs

        # keep track of DRMAA session_id (for resubmissions)
        self.session_id = session_id

        # save useful mapping
        self.jobid_to_job = {}
        for job in jobs:
            self.jobid_to_job[job.name] = job

        # determines in which interval to check if jobs are alive
        local_heart = multiprocessing.Process(target=heart_beat, args=(-1, self.home_address, -1, "", 15))
        local_heart.start()

        print "using the NEW and SHINY ZMQ layer"

        # main loop
        while not self.all_jobs_done():
    
            msg_str = self.socket.recv()
            msg = zloads(msg_str)

            return_msg = ""

            job_id = msg["job_id"]

            # only if its not the local beat
            if job_id != -1:

                job = self.jobid_to_job[job_id] 
                print datetime.now().ctime(), msg

                if msg["command"] == "fetch_input":
                    return_msg = self.jobid_to_job[job_id]

                if msg["command"] == "store_output":
                    # be nice
                    return_msg = "thanks"

                    # store tmp job object
                    tmp_job = msg["data"]

                    # copy relevant fields
                    job.ret = tmp_job.ret
                    job.exception = tmp_job.exception
                    
                    # is assigned in submission process and not written back server-side
                    #job.log_stdout_fn = tmp_job.log_stdout_fn 
                    job.timestamp = datetime.now()

                if msg["command"] == "heart_beat":
                    job.heart_beat = msg["data"]
                    job.log_file = msg["data"]["log_file"]
                    
                    # keep track of mem and cpu
                    try:
                        job.track_mem.append(float(job.heart_beat["memory"]))
                        job.track_cpu.append(float(job.heart_beat["cpu_load"]))
                    except:
                        print "error decoding heart-beat" 
                    
                    return_msg = "all good"

                    job.timestamp = datetime.now()

                if msg["command"] == "get_job":
                    # serve job for display
                    return_msg = job
                else:
                    # update host name
                    job.host_name = msg["host_name"]

            
            else:
                # run check
                self.check_if_alive()

                if msg["command"] == "get_jobs":
                    # serve list of jobs for display
                    return_msg = self.jobs


            # send back compressed response
            self.socket.send(zdumps(return_msg))

        local_heart.terminate()


    def check_if_alive(self):
        """
        check if jobs are alive and determine cause of death if not
        """


        for job in self.jobs:
            
            # noting was returned yet 
            if job.ret == None:

                # exclude first-timers
                if job.timestamp != None:
                    
                    # only check heart-beats if there was a long delay
                    current_time = datetime.now()
                    time_delta = current_time - job.timestamp
    
                    if time_delta.seconds > 90:
                        
                        # could be out-of-memory    
                        if job.is_out_of_memory():
                            print "job was out of memory"
                            job.cause_of_death = "out_of_memory"
    
                        else:
                            #TODO: check if node is reachable
                            #TODO: check if network hangs, wait some more if so
                            print "job died for unknown reason"
                            job.cause_of_death = "unknown"

                
            else:
                
                # could have been an exception, we check right away
                if job.is_out_of_memory():
                    print "job was out of memory"
                    job.cause_of_death = "out_of_memory"
                    job.ret = None
                    
                elif isinstance(job.ret, Exception):
                    print "job encountered exception, will not resubmit"
                    job.cause_of_death = "exception"
                    send_error_mail(job)
                    job.ret = "job dead (with non-memory related exception)"


            # attempt to resubmit            
            if job.cause_of_death == "out_of_memory" or job.cause_of_death == "unknown":

                print "creating error report"

                # send report
                send_error_mail(job)
                    
                # try to resubmit
                if not handle_resubmit(self.session_id, job):
                    print "giving up on job"
                    job.ret = "job dead"
                    
                # break out of loop to avoid too long delay
                break



    def all_jobs_done(self):
        """
        checks for all jobs if they are done
        """

        for job in self.jobs:
            # exceptions will be handled in check_if_alive
            if job.ret == None or isinstance(job.ret, Exception):
                return False

        return True





def handle_resubmit(session_id, job):
    """
    heuristic to determine if the job should be resubmitted

    side-effect: 
    job.num_resubmits incremented
    job.h_vmem will be doubled, if cause was 'out_of_memory'
    """


    # reset some fields
    job.timestamp = None
    job.heart_beat = None
    

    if job.num_resubmits < 3:

        print "looks like job died an unnatural death, resubmitting (previous resubmits = %i)" % (job.num_resubmits)

        if job.cause_of_death == "out_of_memory":
            # double memory
            job.alter_allocated_memory(2.0)

        else:

            # remove node from white_list
            node_name = "all.q@" + job.host_name
            if job.white_list.count(node_name) > 0:
                job.white_list.remove(node_name)

        # increment number of resubmits
        job.num_resubmits += 1
        job.cause_of_death = ""
        
        resubmit(session_id, job)
        
        return True

    else:
        
        return False


def resubmit(session_id, job):
    """
    encapsulate creation of session for multiprocessing
    """

    print "starting resubmission process"

    # append to session
    session = drmaa.Session()
    session.initialize(session_id)
    
    # try to kill off old job
    try:
        # TODO: ask SGE more questions about job status etc (maybe re-integrate 
        # TODO: make sure this works
        # some of the other StatusChecker code
        session.control(job.jobid, drmaa.JobControlAction.TERMINATE)
        print "zombie job killed"
    except Exception:
        print "could not kill job with SGE id", job.jobid
    
    # create new job
    append_job_to_session(session, job)
    session.exit()



#####################################################################
# MapReduce Interface
#####################################################################



def pg_map(f, input_list, param=None, local=False, maxNumThreads=1, mem="5G"):
    """
    provides a generic map function
    """

    jobs=[]

    # construct jobs
    for input in input_list:
        job = KybJob(f, [input], param=param)
        job.h_vmem = mem

        jobs.append(job)
        

    # process jobs
    processed_jobs = process_jobs(jobs, local=local, maxNumThreads=maxNumThreads)

    # store results
    results = [job.ret for job in processed_jobs]

    assert(len(jobs) == len(processed_jobs))
    # make sure results are in the same order
    # TODO make reasonable check
    #for idx in range(len(jobs)):
    #    assert(jobs[idx].args == processed_jobs[idx].args)

    return results



class MapReduce(object):
    """
    convenient high-level API for map-reduce interface
    """

    def __init__(self, fun_map, fun_reduce, input_list, param=None, name=None):
        """
        combines all that is needed for map-reduce
        """

        self.fun_map = fun_map
        self.fun_reduce = fun_reduce
        self.input_list = input_list
        self.param = param
        self.name = name

    def wait(self, local=True, max_num_threads=1):
        """
        wait for jobs to finish
        """

        intermediate_results = pg_map(self.fun_map, self.input_list, self.param, local, max_num_threads)

        # apply user-defined reduce function to intermediate result
        result = self.fun_reduce(intermediate_results)

        return result


#####################################################################
# Data persistence
#####################################################################



def zdumps(obj):
    """
    dumps pickleable object into zlib compressed string
    """
    return zlib.compress(cPickle.dumps(obj,cPickle.HIGHEST_PROTOCOL),9)


def zloads(zstr):
    """
    loads pickleable object from zlib compressed string
    """
    return cPickle.loads(zlib.decompress(zstr)) 


def save(filename, myobj):
    """
    Save myobj to filename using pickle
    """
    try:
        f = gzip.GzipFile(filename, 'wb')
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
        f = gzip.GzipFile(filename, 'rb')
    except IOError, details:
        sys.stderr.write('File ' + filename + ' cannot be read\n')
        sys.stderr.write(details)
        return

    myobj = cPickle.load(f)
    f.close()
    return myobj


################################################################
#      Some handy functions
################################################################


def send_error_mail(job):
    """
    send out diagnostic email
    """
    

    # create message
    msg = MIMEMultipart()
    msg["subject"] = "PYTHONGRID error " + str(job.name)
    msg["From"] = "pythongrid"
    msg["To"] = "pythongrid user"
    
    
    # compose error message
    body_text = ""

    body_text += "job " + str(job.name) + "\n"
    body_text += "last timestamp: " + str(job.timestamp) + "\n"
    body_text += "num_resubmits: " + str(job.num_resubmits) + "\n"
    body_text += "cause_of_death: " + str(job.cause_of_death) + "\n"

    if job.heart_beat:
        body_text += "last memory usage: " + str(job.heart_beat["memory"]) + "\n"
        body_text += "last cpu load: " + str(job.heart_beat["cpu_load"]) + "\n"
        
    body_text += "requested memory: " + str(job.h_vmem) + "\n"
    body_text += "host: " + str(job.host_name) + "\n\n"
    
    if isinstance(job.ret, Exception):
        body_text += "job encountered exception: " + str(job.ret) + "\n"
        body_text += "stacktrace: " + str(job.exception) + "\n\n"
    
    print body_text
    
    body_msg = MIMEText(body_text)
    msg.attach(body_msg)
    
    
    # attach log file
    if job.heart_beat:
        log_file = open(job.heart_beat["log_file"], "r")
        log_file_attachement = MIMEText(log_file.read())
        log_file.close()
        
        msg.attach(log_file_attachement)


    # if matplotlib is installed
    if MATPLOTLIB_PRESENT:
        
        #TODO: plot to cstring directly (some code is there)
        #imgData = cStringIO.StringIO()
        #pylab.savefig(imgData, format='png')

        # rewind the data
        #imgData.seek(0)
        #pylab.savefig(imgData, format="png")

        # attack mem plot        
        img_mem_fn = "/tmp/" + job.name + "_mem.png"

        pylab.figure()
        pylab.plot(job.track_mem, "-o")
        pylab.title("memory usage")
        pylab.savefig(img_mem_fn)
        
        img_mem = open(img_mem_fn, "rb")
        img_mem_attachement = MIMEImage(img_mem.read())
        img_mem.close()
        
        msg.attach(img_mem_attachement)


        # attach cpu plot
        img_cpu_fn = "/tmp/" + job.name + "_cpu.png"

        pylab.figure()
        pylab.plot(job.track_cpu, "-o")
        pylab.title("cpu load")
        pylab.savefig(img_cpu_fn)
        
        img_cpu = open(img_cpu_fn, "rb")
        img_cpu_attachement = MIMEImage(img_cpu.read())
        img_cpu.close()
        
        msg.attach(img_cpu_attachement)


    # send out report
    #TODO: take this from config file, examine problem with msg length
    """
  File "/fml/ag-raetsch/home/cwidmer/svn/tools/python/pythongrid/pythongrid.py", line 1246, in send_error_mail
    s.sendmail("cwidmer@tuebingen.mpg.de", "ckwidmer@gmail.com", msg.as_string())
  File "/usr/lib/python2.6/smtplib.py", line 713, in sendmail
    raise SMTPDataError(code, resp)
    smtplib.SMTPDataError: (552, 'message line is too long')

    """
    s = smtplib.SMTP("mailhost.tuebingen.mpg.de")
    s.sendmail("cwidmer@tuebingen.mpg.de", "ckwidmer@gmail.com", msg.as_string()[0:5000])
    s.quit()




def _VmB(VmKey, pid):
    """
    get various mem usage properties of process with id pid in MB
    """

    _proc_status = '/proc/%d/status' % pid

    _scale = {'kB': 1.0/1024.0, 'mB': 1.0,
              'KB': 1.0/1024.0, 'MB': 1.0}

     # get pseudo file  /proc/<pid>/status
    try:
        t = open(_proc_status)
        v = t.read()
        t.close()
    except:
        return 0.0  # non-Linux?
     # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
    i = v.index(VmKey)
    v = v[i:].split(None, 3)  # whitespace
    if len(v) < 3:
        return 0.0  # invalid format?
     # convert Vm value to bytes
    return float(v[1]) * _scale[v[2]]


def get_memory_usage(pid):
    """
    return memory usage in Mb.
    """

    return _VmB('VmSize:', pid)


def get_cpu_load(pid):
    """
    return cpu usage of process
    """

    command = "ps h -o pcpu -p %d" % (pid)

    try:
        ps_pseudofile = os.popen(command)
        info = ps_pseudofile.read()
        ps_pseudofile.close()

        cpu_load = info.strip()
    except Exception, detail:
        print "getting cpu info failed:", detail
        cpu_load = "non-linux?"

    return cpu_load


def get_white_list():
    """
    parses output of qstat -f to get list of nodes
    """

    #TODO refactor this, its specific to our naming scheme
    try:
        qstat = os.popen("qstat -f")

        node_names = []
        norm_loads = []

        for line in qstat:

            # we kick out all old nodes, node1XX
            if line.startswith("all.q@") and not line.startswith("all.q@node1"):
                tokens = line.strip().split()
                node_name = tokens[0]

                if len(tokens) == 6:
                    continue
            
                slots = float(tokens[2].split("/")[2])
                cpu_load = float(tokens[3])

                norm_load = cpu_load/slots 

                node_names.append(node_name)
                norm_loads.append(norm_load)

        qstat.close()

        return node_names

    except Exception, details:
        print "getting whitelist failed", details
        return ""


def argsort(seq):
    """
    argsort in basic python
    """

    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)



################################################################
#      The following code will be executed on the cluster      #
################################################################


def heart_beat(job_id, address, parent_pid=-1, log_file="", wait_sec=45):
    """
    will send reponses to the server with
    information about the current state of
    the process
    """

    while True:
        status = get_job_status(parent_pid)
        status["log_file"] = log_file
        send_zmq_msg(job_id, "heart_beat", status, address)
        time.sleep(wait_sec)


def get_job_status(parent_pid):
    """
    script to determine the status of the current 
    worker and its machine (currently not cross-platform)
    """

    status_container = {}

    if parent_pid != -1:
        status_container["memory"] = get_memory_usage(parent_pid)
        status_container["cpu_load"] = get_cpu_load(parent_pid)

    return status_container


def run_job(job_id, address):
    """
    This is the code that is executed on the cluster side.

    @param job_id: unique id of job
    @type job_id: string
    """

    wait_sec = random.randint(0, 5)
    print "waiting %i seconds before starting" % (wait_sec)
    time.sleep(wait_sec)

    job = send_zmq_msg(job_id, "fetch_input", None, address)

    print "input arguments loaded, starting computation", job.args

    parent_pid = os.getpid()

    # create heart beat process
    heart = multiprocessing.Process(target=heart_beat, args=(job_id, address, parent_pid, job.log_stdout_fn, 10))

    print "starting heart beat"

    heart.start()

    # change working directory
    print "changing working directory"
    if 1:
        if job.working_dir is not None:
            print "Changing working directory: %s" % job.working_dir
            os.chdir(job.working_dir)

    print "executing job"

    # run job
    job.execute()

    # send back result
    thank_you_note = send_zmq_msg(job_id, "store_output", job, address)
    print thank_you_note

    # stop heartbeat
    heart.terminate()



def send_zmq_msg(job_id, command, data, address):
    """
    simple code to send messages back to host
    (and get a reply back)
    """

    context = zmq.Context()
    zsocket = context.socket(zmq.REQ)
    zsocket.connect(address)

    host_name = socket.gethostname()
    ip_address = socket.gethostbyname(host_name)

    msg_container = {}
    msg_container["job_id"] = job_id
    msg_container["host_name"] = host_name
    msg_container["ip_address"] = ip_address
    msg_container["command"] = command
    msg_container["data"] = data

    msg_string = zdumps(msg_container)

    zsocket.send(msg_string)
    msg = zloads(zsocket.recv())

    return msg


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
            run_job(args[0], args[1])
        except getopt.error, msg:
            raise Usage(msg)


    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"

        return 2

if __name__ == "__main__":
    main(sys.argv)
