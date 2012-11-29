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
import re
import sqlite3
import sys
from tempfile import NamedTemporaryFile

import drmaa


class Job(object):
    """
    Central entity that wraps a function and its data. Basically,
    a job consists of a function, its argument list, its
    keyword list and a field "ret" which is filled, when
    the execute method gets called
    """

    __slots__ = ('_f', 'args', 'jobid', 'kwlist', 'cleanup', 'ret', 'exception', 'environment', 'replace_env', 'working_dir', 'num_slots', 'mem_free', 'white_list', 'path',
                 'db_dir', 'name')

    def __init__(self, f, args, kwlist=None, cleanup=True, mem_free="1G", name='pythongrid_job', num_slots=1, db_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/'):
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
        self.path = None
        self.db_dir = db_dir
        self.name = name

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
        self.path = os.path.dirname(m.__file__)

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
        # Currently commented out until we enable mem_free on the cluster
        # if self.mem_free:
        #     ret += " -l mem_free={}".format(self.mem_free)
        if self.num_slots and self.num_slots > 1:
            ret += " -pe smp {}".format(self.num_slots)
        if self.white_list:
            ret += " -l h={}".format('|'.join(self.white_list))

        return ret


def _submit_jobs(jobs, pickle_db, temp_dir='/scratch', white_list=None):
    """
    Method used to send a list of jobs onto the cluster.
    @param jobs: list of jobs to be executed
    @type jobs: list<Job>
    """

    session = drmaa.Session()
    session.initialize()
    jobids = []

    for job_num, job in enumerate(jobs):
        # set job white list
        job.white_list = white_list

        # append jobs
        jobid = _append_job_to_session(session, job, pickle_db, job_num, temp_dir=temp_dir)
        jobids.append(jobid)

    sid = session.contact
    session.exit()

    return (sid, jobids)


def _append_job_to_session(session, job, pickle_db, job_num, temp_dir='/scratch/'):
    """
    For an active session, append new job
    based on information stored in job object

    side-effects:
    - job.jobid set to jobid determined by grid-engine
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

    jt.remoteCommand = _clean_path(os.path.abspath(__file__))
    jt.args = [pickle_db, job_num, job.path]
    jt.nativeSpecification = job.native_specification
    jt.outputPath = ":" + temp_dir
    jt.errorPath = ":" + temp_dir

    jobid = session.runJob(jt)

    # set job fields that depend on the jobid assigned by grid engine
    job.jobid = jobid

    print('Your job {} has been submitted with id {}'.format(job.name, jobid), file=sys.stderr)

    session.deleteJobTemplate(jt)

    return jobid


def _collect_jobs(sid, jobids, joblist, con, db_filename, temp_dir='/scratch/', wait=True):
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
    # print("success: all jobs finished", file=sys.stderr)
    s.exit()

    # attempt to collect results
    job_output_list = []
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
                print(file=sys.stderr)

            #remove files
            elif job.cleanup:
                # print("cleaning up:", log_stdout_fn)
                os.remove(log_stdout_fn)

                # print("cleaning up:", log_stderr_fn)
                os.remove(log_stderr_fn)

        except Exception as detail:
            print("Error while unpickling output for pythongrid job from sqlite database {}".format(db_filename), file=sys.stderr)
            print("This could caused by a problem with the cluster environment, imports or environment variables.", file=sys.stderr)
            print("Check log files for more information: ", file=sys.stderr)
            print("stdout:", log_stdout_fn, file=sys.stderr)
            print("stderr:", log_stderr_fn, file=sys.stderr)
            raise detail

        job_output_list.append(job_output)

    return job_output_list


def process_jobs(jobs, db_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/', temp_dir='/scratch/', white_list=None):
    """
    Take a list of jobs and process them on the cluster.

    @param db_dir: Directory to store the temporary sqlite database used behind the scenes. Must be on the SAN so that all nodes can access it.
    @type db_dir: C{basestring}
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    @param white_list: If specified, limit nodes used to only those in list.
    @type white_list: C{list} of C{basestring}
    """

    # Get new filename for temporary database
    with NamedTemporaryFile(dir=db_dir, delete=False) as temp_db_file:
        db_filename = temp_db_file.name

    # Create new sqlite database with pickled jobs
    con = sqlite3.connect(db_filename)

    # Create tables
    with con:
        con.execute("CREATE TABLE job(id INTEGER, data BLOB)")
        con.execute("CREATE TABLE output(id INTEGER, data BLOB)")

    for job_id, job in enumerate(jobs):
        _zsave_db(job, con, 'job', job_id)

    # Submit jobs to cluster
    sids, jobids = _submit_jobs(jobs, db_filename, white_list=white_list, temp_dir=temp_dir)

    # Retrieve outputs
    return _collect_jobs(sids, jobids, jobs, con, db_filename, temp_dir=temp_dir)


#####################################################################
# MapReduce Interface
#####################################################################
def pg_map(f, args_list, cleanup=True, mem_free="1G", name='pythongrid_job', num_slots=1, db_dir='/home/nlp-text/dynamic/dblanchard/pythongrid_dbs/',
           temp_dir='/scratch/', white_list=None):
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
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    @param white_list: If specified, limit nodes used to only those in list.
    @type white_list: C{list} of C{basestring}
    """

    # construct jobs
    jobs = [Job(f, [args], cleanup=cleanup, mem_free=mem_free, name='{}{}'.format(name, job_num), num_slots=num_slots, db_dir=db_dir) for job_num, args in enumerate(args_list)]

    # process jobs
    job_results = process_jobs(jobs, db_dir=db_dir, temp_dir=temp_dir, white_list=white_list)

    assert(len(jobs) == len(job_results))

    return job_results


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

    # Pickle the obj
    pickled_data = bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)

    # Insert the pickled data into the database
    with con:
        con.execute("INSERT INTO {}(id, data) VALUES (:id, :data)".format(table), {"id": job_num, "data": sqlite3.Binary(pickled_data)})


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
    with con:
        cur = con.cursor()
        cur.execute('SELECT data FROM {} WHERE id={}'.format(table, job_num))
        pickled_data = cur.fetchone()[0]
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

    print("Running job...", end="", file=sys.stderr)
    sys.stderr.flush()
    job.execute()
    print("done", file=sys.stderr)

    print("Writing output to database for job {}...".format(job_num), end="", file=sys.stderr)
    sys.stderr.flush()
    _zsave_db(job.ret, con, 'output', job_num)
    print("done", file=sys.stderr)

    con.close()


def main():
    """
    Parse the command line inputs and call _run_job
    """

    # Get command line arguments
    parser = argparse.ArgumentParser(description="This wrapper script will run a pickled Python function on some pickled data in a sqlite3 database, " +
                                                 "and write the results back to the database. You almost never want to run this yourself.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     conflict_handler='resolve')
    parser.add_argument('pickle_db', help='SQLite3 database containing the pickled job, the input data, and a table for output data.')
    parser.add_argument('job_number', help='Which job number should be run. Dictates which input data is read from database and where output data is stored.', type=int)
    parser.add_argument('module_dir', help='Directory that contains module containing pickled function. This will get added to PYTHONPATH temporarily.')
    args = parser.parse_args()

    print("Appended {} to PYTHONPATH".format(args.module_dir), file=sys.stderr)
    sys.path.append(_clean_path(args.module_dir))

    # Process the database and get job started
    _run_job(args.pickle_db, args.job_number)


if __name__ == "__main__":
    main()
