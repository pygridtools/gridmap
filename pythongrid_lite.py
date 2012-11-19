#! /usr/bin/env python
# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Copyright (C) 2008-2012 Max-Planck-Society
#
# Mostly rewritten by Dan Blanchard, November 2012

"""
pythongrid provides a high level front-end to DRMAA-python.

This module provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.

"""

import argparse
import bz2
import cPickle
import gzip
import inspect
import os
import os.path
import random
import sys
import time
import traceback
import uuid

import drmaa
# import configuration
from pythongrid_cfg import CFG


class Job(object):
    """
    Central entity that wraps a function and its data. Basically,
    a job consists of a function, its argument list, its
    keyword list and a field "ret" which is filled, when
    the execute method gets called
    """

    def __init__(self, f, args, kwlist=None, param=None, cleanup=True):
        """
        constructor of Job

        @param f: a function, which should be executed.
        @type f: function
        @param args: argument list of function f
        @type args: list
        @param kwlist: dictionary of keyword arguments for f
        @type kwlist: dict
        @param cleanup: flag that determines the cleanup of input and log file
        @type cleanup: boolean
        """

        self._f = None
        self.function = f
        self.args = args
        self.jobid = -1
        self.kwlist = kwlist if kwlist is not None else {}
        self.cleanup = cleanup
        self.ret = None
        self.exception = None
        self.environment = None
        self.replace_env = False
        self.working_dir = os.getcwd()
        self.pe = ""
        self.white_list = []

        if param is not None:
            self.__set_parameters(param)

        outdir = os.path.expanduser(CFG['TEMPDIR'])
        if not os.path.isdir(outdir):
            print '%s does not exist. Please create a directory' % outdir
            raise Exception()

        self.name = 'pg_{}'.format(uuid.uuid1())
        self.jobid = ""

    @property
    def function(self):
        ''' Function this job will execute. '''
        return self._f

    @function.setter
    def function(self, f):
        """
        setter for function that carefully takes care of
        namespace, avoiding __main__ as a module
        """

        m = inspect.getmodule(f)

        # if module is not __main__, all is good
        if m.__name__ != "__main__":
            self._f = f

        else:

            # determine real module name
            mn = os.path.splitext(os.path.basename(m.__file__))[0]

            # make sure module is present
            __import__(mn)

            # get module
            mod = sys.modules[mn]

            # set function from module
            # self._f = mod.__getattribute__(f.__name__)
            self._f = getattr(mod, f.__name__)

    def __set_parameters(self, param):
        """
        method to set parameters from dict
        """

        assert(param is not None)

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
            self.ret = self.function(*self.args, **self.kwlist)
        except Exception as e:
            print "exception encountered"
            print "type: {}".format(type(e))
            print "line number:", sys.exc_info()[2].tb_lineno
            print e
            print "========="
            self.exception = traceback.format_exc()
            self.ret = e

            print self.exception
            traceback.print_exc(file=sys.stdout)

    @property
    def native_specification(self):
        """
        define python-style getter
        """

        ret = ""

        if self.name:
            ret += " -N {}".format(self.name)
        if self.pe:
            ret += " -pe {}".format(self.pe)
        if self.white_list:
            ret += " -l h={}".format('|'.join(self.white_list))

        return ret


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
    jt.nativeSpecification = job.native_specification
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
                print '{}'.format(type(retJob.exception))
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

        except Exception as detail:
            print "error while unpickling file: " + job.outputfile
            print "this could caused by a problem with the cluster environment, imports or environment variables"
            print "check log files for more information: "
            print "stdout:", log_stdout_fn
            print "stderr:", log_stderr_fn
            print detail

    return retJobs


def process_jobs(jobs):
    """
    Director function to decide whether to run on the cluster or locally
    local: local or cluster processing
    """

    # get list of trusted nodes
    white_list = CFG['WHITELIST']

    # ignoring jobid field attached to each job object
    sid = submit_jobs(jobs, home_address, white_list)[0]

    # handling of inputs, outputs and heartbeats
    # TODO: implement this

    return jobs


#####################################################################
# MapReduce Interface
#####################################################################
def pg_map(f, args_list, param=None, mem_free="1G"):
    """
    provides a generic map function
    """

    # construct jobs
    jobs = [Job(f, [args], param=param, mem_free=mem_free) for args in args_list]

    # process jobs
    processed_jobs = process_jobs(jobs)

    # store results
    results = [job.ret for job in processed_jobs]

    assert(len(jobs) == len(processed_jobs))

    return results


#####################################################################
# Data persistence
#####################################################################
def zdumps(obj):
    """
    dumps pickleable object into bz2 compressed string
    """
    return bz2.compress(cPickle.dumps(obj, cPickle.HIGHEST_PROTOCOL), 9)


def zloads(zstr):
    """
    loads pickleable object from bz2 compressed string
    """
    return cPickle.loads(bz2.decompress(zstr))


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
#      The following code will be executed on the cluster      #
################################################################
def run_job(job_id, address):
    """
    This is the code that is executed on the cluster side.

    @param job_id: unique id of job
    @type job_id: string
    """

    wait_sec = random.randint(0, 5)
    print "waiting %i seconds before starting" % (wait_sec)
    time.sleep(wait_sec)

    try:
        job = send_zmq_msg(job_id, "fetch_input", None, address)
    except Exception as e:
        # here we will catch errors caused by pickled objects
        # of classes defined in modules not in PYTHONPATH
        print e

        # send back exception

        ## This doesn't seem to work (either the old version or the new one)
        thank_you_note = send_zmq_msg(job_id, "store_output", e, address)
        print thank_you_note

        return

    print "input arguments loaded, starting computation", job.args

    parent_pid = os.getpid()

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


def main(argv=None):
    """
    Parse the command line inputs and call run_job

    @param argv: list of arguments
    @type argv: list of strings
    """

    # Get command line arguments
    parser = argparse.ArgumentParser(description="This wrapper script will run a pickled Python function on some pickled data in a sqlite3 database, " +
                                                 "and write the results back to the database. You almost never want to run this yourself.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     conflict_handler='resolve')
    parser.add_argument('pickle_db', help='SQLite3 database containing the pickled function, the input data, and a table for output data.')
    parser.add_argument('job_number', help='Which job number should be run. Dictates which input data is read from database and where output data is stored.')
    args = parser.parse_args(argv if argv is not None else sys.argv)

    # Process the database and get started
    run_job(args.pickle_db, args.job_number)


if __name__ == "__main__":
    main()
