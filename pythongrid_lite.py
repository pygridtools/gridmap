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

from __future__ import print_function, unicode_literals

import argparse
import bz2
import cPickle as pickle
import inspect
import os
import os.path
import random
import re
import sqlite3
import sys
import time
import traceback
import uuid
from tempfile import NamedTemporaryFile

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

    def __init__(self, f, args, kwlist=None, cleanup=True, mem_free="1G", name=None, num_slots=1, db_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/'):
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
        @param mem_free: Estimate of how much memory this job will need (for scheduling)
        @type mem_free: C{basestring}
        @param name: Name to give this job
        @type name: C{basestring}
        @param num_slots: Number of slots this job should use.
        @type num_slots: C{int}
        @param db_dir: Directory to store the temporary sqlite database used behind the scenes. Must be on the SAN so that all nodes can access it.
        @type db_dir: C{basestring}
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
        self.num_slots = num_slots
        self.mem_free = mem_free
        self.white_list = []
        self._path = None
        self.db_dir = db_dir
        self.name = 'pythongrid_{}'.format(uuid.uuid1()) if not name else name

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
        self._path = os.path.dirname(m.__file__)

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
            self.ret = e

    @property
    def native_specification(self):
        """
        define python-style getter
        """

        ret = ""

        if self.name:
            ret += " -N {}".format(self.name)
        if self.mem_free:
            ret += " -l mem_free={}".format(self.mem_free)
        if self.num_slots:
            ret += " -pe smp {}".format(self.num_slots)
        if self.white_list:
            ret += " -l h={}".format('|'.join(self.white_list))

        return ret


def submit_jobs(jobs, pickle_db, white_list=""):
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
        job.pickle_db = pickle_db

        # append jobs
        jobid = append_job_to_session(session, job)
        jobids.append(jobid)

    sid = session.contact
    session.exit()

    return (sid, jobids)


def append_job_to_session(session, job, db_dir='/scratch/'):
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

    jt.remoteCommand = os.path.abspath(__file__)
    jt.args = [job.name, job.pickle_db]
    jt.joinFiles = True
    jt.nativeSpecification = job.native_specification
    jt.outputPath = ":" + db_dir
    jt.errorPath = ":" + temp_dir

    jobid = session.runJob(jt)

    # set job fields that depend on the jobid assigned by grid engine
    job.jobid = jobid
    job.log_stdout_fn = (temp_dir + job.name + '.o' + jobid)
    job.log_stderr_fn = (temp_dir + job.name + '.e' + jobid)

    print('Your job {} has been submitted with id {}'.format(job.name, jobid), file=sys.stderr)
    print("stdout: {}".format(job.log_stdout_fn), file=sys.stderr)
    print("stderr: {}".format(job.log_stderr_fn), file=sys.stderr)
    print(file=sys.stderr)

    session.deleteJobTemplate(jt)

    return jobid


def collect_jobs(sid, jobids, joblist, con, temp_dir='/scratch/', wait=True):
    """
    Collect the results from the jobids, returns a list of Jobs

    @param sid: session identifier
    @type sid: string returned by cluster
    @param jobids: list of job identifiers returned by the cluster
    @type jobids: list of strings
    @param con: Open connection to the sqlite3 database where the results will be stored.
    @type con: C{sqlite3.Connection}
    @param wait: Wait for jobs to finish?
    @type wait: Boolean, defaults to False
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
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
    # print("success: all jobs finished")
    s.exit()

    # attempt to collect results
    ret_jobs = []
    for ix, job in enumerate(joblist):

        log_stdout_fn = (os.path.join(temp_dir, job.name + '.o' + jobids[ix]))
        log_stderr_fn = (os.path.join(temp_dir, job.name + '.e' + jobids[ix]))

        try:
            job_output = _zload_db(con, 'output', ix)

            #print exceptions
            if isinstance(job_output, Exception):
                print("Exception encountered in job with log file:", file=sys.stderr)
                print(log_stdout_fn, file=sys.stderr)
                print(job_output, file=sys.stderr)

            #remove files
            elif job.cleanup:
                # print("cleaning up:", log_stdout_fn)
                os.remove(log_stdout_fn)

                # print("cleaning up:", log_stderr_fn)
                os.remove(log_stderr_fn)

        except Exception as detail:
            print("Error while unpickling output for pythongrid job!", file=sys.stderr)
            print("This could caused by a problem with the cluster environment, imports or environment variables.", file=sys.stderr)
            print("Check log files for more information: ", file=sys.stderr)
            print("stdout:", log_stdout_fn, file=sys.stderr)
            print("stderr:", log_stderr_fn, file=sys.stderr)
            raise detail

    return ret_jobs


def process_jobs(jobs, db_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/', temp_dir='/scratch/'):
    """
    Director function to decide whether to run on the cluster or locally
    local: local or cluster processing

    @param db_dir: Directory to store the temporary sqlite database used behind the scenes. Must be on the SAN so that all nodes can access it.
    @type db_dir: C{basestring}
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    """

    # get list of trusted nodes
    white_list = CFG['WHITELIST']

    # Get new filename for temporary database
    with NamedTemporaryFile(dir=db_dir, delete=False) as temp_db_file:
        db_filename = temp_db_file.name

    # Create new sqlite database with pickled jobs
    con = sqlite3.connect(db_filename)
    for job_id, job in enumerate(jobs):
        _zsave_db(con, job, 'job', job_id)

    # Submit jobs to cluster
    sid, jobids = submit_jobs(jobs, db_filename, white_list)

    # Retrieve outputs
    return collect_jobs(sids, jobids, jobs, con, temp_dir=temp_dir)


#####################################################################
# MapReduce Interface
#####################################################################
def pg_map(f, args_list, cleanup=True, mem_free="1G", name=None, num_slots=1, db_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/'):
    """
    provides a generic map function
    @param f: The function to map on args_list
    @type f: C{function}
    @param args_list: List of arguments to pass to f
    @type args_list: C{args_list}
    @param cleanup: flag that determines the cleanup of input and log file
    @type cleanup: boolean
    @param mem_free: Estimate of how much memory each job will need (for scheduling)
    @type mem_free: C{basestring}
    @param name: Base name to give each job (will have number add to end)
    @type name: C{basestring}
    @param num_slots: Number of slots each job should use.
    @type num_slots: C{int}
    @param db_dir: Directory to store the temporary sqlite database used behind the scenes. Must be on the SAN so that all nodes can access it.
    @type db_dir: C{basestring}
    """

    # construct jobs
    jobs = [Job(f, [args], cleanup=cleanup, mem_free=mem_free, name='{}{}'.format(name, job_num), num_slots=num_slots, temp_dir=db_dir) for job_num, args in enumerate(args_list)]

    # process jobs
    processed_jobs = process_jobs(jobs)

    # store results
    results = [job.ret for job in processed_jobs]

    assert(len(jobs) == len(processed_jobs))

    return results


#####################################################################
# Data persistence
#####################################################################
def _clean_path(path):
    ''' Replace all weird SAN paths with normal paths '''

    path = re.sub(r'/\.automount/\w+/SAN/NLP/(\w+)-(dynamic|static)', r'/home/nlp-\1/\2', path)
    path = re.sub(r'/\.automount/[^/]+/SAN/Research/HomeResearch', '/home/research', path)
    return path


def _zsave_db(obj, con, table, job_num):
    """
    Saves an object/function as bz2-compressed pickled data in an sqlite database table

    @param obj: The object/function to store.
    @type obj: C{object} or C{function}
    @param con: An open connection to the sqlite database
    @type con: C{sqlite3.Connection}
    @param table: The name of the table to retrieve data from.
    @type table: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """

    # Create table if necessary
    with con:
        con.execute("CREATE TABLE IF NOT EXISTS {} (id INTEGER, data BLOB)".format(table))

    # Pickle the obj
    pickled_data = bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)

    # Insert the pickled data into the database
    with con:
        con.execute("INSERT INTO {}(id, data) VALUES (?, ?)".format(table), (job_num, sqlite3.Binary(pickled_data)))


def _zload_db(con, table, job_num):
    """
    Loads bz2-compressed pickled object from sqlite database table

    @param con: An open connection to the sqlite database
    @type con: C{sqlite3.Connection}
    @param table: The name of the table to retrieve data from.
    @type table: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """

    pickled_data = next(con.execute('SELECT data FROM {} WHERE id == {}'.format(table, job_num)))['data']
    return pickle.loads(bz2.decompress(str(pickled_data)))


################################################################
#      The following code will be executed on the cluster      #
################################################################
def _run_job(pickle_db, job_num):
    """
    Execute the pickled job and produce pickled output (all in the SQLite3 database).

    @param pickle_db: Path to SQLite3 database that must contain tables called "job" and "output" where both contains two columns: "id" and "data"
                      (corresponding to the IDs and the actual job/output for each job).
    @type pickle_db: C{basestring}
    @param job_num: The index for this job's content in the job and output tables.
    @type job_num: C{int}

    """
    con = sqlite3.connect(pickle_db)

    print("Loading job...", end="", file=sys.stderr)
    sys.stderr.flush()
    job = _zload_db(con, 'job', job_num)
    print("done", file=sys.stderr)

    print("Running job...")
    sys.stderr.flush()
    job.execute()
    print("done", file=sys.stderr)

    print("Writing output to database...")
    sys.stderr.flush()
    _zsave_db(job.ret, con, 'output', job_num)
    print("done", file=sys.stderr)

    con.close()


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
    parser.add_argument('pickle_db', help='SQLite3 database containing the pickled job, the input data, and a table for output data.')
    parser.add_argument('job_number', help='Which job number should be run. Dictates which input data is read from database and where output data is stored.')
    parser.add_argument('module_dir', help='Directory that contains module containing pickled function. This will get added to PYTHONPATH temporarily.')
    args = parser.parse_args(argv if argv is not None else sys.argv)

    sys.path.append(_clean_path(args.module_dir))

    # Process the database and get job started
    run_job(args.pickle_db, args.job_number)


if __name__ == "__main__":
    main()
