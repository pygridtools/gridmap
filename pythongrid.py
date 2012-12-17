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
import sys
import traceback
import uuid
from random import random
from time import sleep

import drmaa
import MySQLdb as mysql


# Limit on the number of connection attempts to the MySQL server
MAX_SQL_ATTEMPTS = 50


class Job(object):
    """
    Central entity that wraps a function and its data. Basically, a job consists of a function, its argument list, its keyword list and a field "ret" which is filled, when
    the execute method gets called.

    @note: This can only be used to wrap picklable functions (i.e., those that are defined at the module or class level).
    """

    __slots__ = ('_f', 'args', 'jobid', 'kwlist', 'cleanup', 'ret', 'exception', 'environment', 'replace_env', 'working_dir', 'num_slots', 'mem_free', 'white_list', 'path',
                 'uniq_id', 'name', 'queue')

    def __init__(self, f, args, kwlist=None, cleanup=True, mem_free="1G", name='pythongrid_job', num_slots=1, queue='nlp.q'):
        """
        Initializes a new Job.

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
        @param queue: SGE queue to schedule job on.
        @type queue: C{basestring}
        """

        self.path = None
        self._f = None
        self.function = f
        self.args = args
        self.jobid = -1
        self.kwlist = kwlist if kwlist is not None else {}
        self.cleanup = cleanup
        self.ret = None
        self.environment = None
        self.replace_env = False
        self.working_dir = os.getcwd()
        self.num_slots = num_slots
        self.mem_free = mem_free
        self.white_list = []
        self.uniq_id = None
        self.name = name
        self.queue = queue

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
        try:
            self.path = _clean_path(os.path.dirname(os.path.abspath(inspect.getsourcefile(f))))
        except TypeError:
            self.path = ''

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
        contain a pickled version of it.
        Input data is removed after execution to save space.
        """
        try:
            self.ret = self.function(*self.args, **self.kwlist)
            del self.args
            del self.kwlist
        except Exception as e:
            self.ret = e
            traceback.print_exc()

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
        if self.queue:
            ret += " -q {}".format(self.queue)

        return ret


def _submit_jobs(jobs, uniq_id, temp_dir='/scratch', white_list=None, quiet=True):
    """
    Method used to send a list of jobs onto the cluster.
    @param jobs: list of jobs to be executed
    @type jobs: c{list} of L{Job}
    @param uniq_id: The unique suffix for the tables corresponding to this job in the database.
    @type uniq_id: C{basestring}
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    @param white_list: List of acceptable nodes to use for scheduling job. If None, all are used.
    @type white_list: C{list} of C{basestring}
    @param quiet: When true, do not output information about the jobs that have been submitted.
    @type quiet: C{bool}
    """

    session = drmaa.Session()
    session.initialize()
    jobids = []

    for job_num, job in enumerate(jobs):
        # set job white list
        job.white_list = white_list

        # append jobs
        jobid = _append_job_to_session(session, job, uniq_id, job_num, temp_dir=temp_dir, quiet=quiet)
        jobids.append(jobid)

    sid = session.contact
    session.exit()

    return (sid, jobids)


def _append_job_to_session(session, job, uniq_id, job_num, temp_dir='/scratch/', quiet=True):
    """
    For an active session, append new job based on information stored in job object. Also sets job.job_id to the ID of the job on the grid.

    @param session: The current DRMAA session with the grid engine.
    @type session: C{drmaa.Session}
    @param job: The Job to add to the queue.
    @type job: L{Job}
    @param uniq_id: The unique suffix for the tables corresponding to this job in the database.
    @type uniq_id: C{basestring}
    @param job_num: The row in the table to store/retrieve data on. This is only non-zero for jobs created via pg_map.
    @type job_num: C{int}
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    @param quiet: When true, do not output information about the jobs that have been submitted.
    @type quiet: C{bool}
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

    jt.remoteCommand = re.sub(r'\.pyc$', '.py', _clean_path(os.path.abspath(__file__)))  # Make sure to use the .py and not the .pyc version of the module.
    jt.args = ['{}'.format(uniq_id), '{}'.format(job_num), job.path, temp_dir]
    jt.nativeSpecification = job.native_specification
    jt.outputPath = ":" + temp_dir
    jt.errorPath = ":" + temp_dir

    jobid = session.runJob(jt)

    # set job fields that depend on the jobid assigned by grid engine
    job.jobid = jobid

    if not quiet:
        print('Your job {} has been submitted with id {}'.format(job.name, jobid), file=sys.stderr)

    session.deleteJobTemplate(jt)

    return jobid


def _collect_jobs(sid, jobids, joblist, con, uniq_id, temp_dir='/scratch/', wait=True):
    """
    Collect the results from the jobids, returns a list of Jobs

    @param sid: session identifier
    @type sid: string returned by cluster
    @param jobids: list of job identifiers returned by the cluster
    @type jobids: list of strings
    @param con: Open connection to the database where the results will be stored.
    @type con: C{MySQLdb.Connection}
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

        log_stdout_fn = os.path.join(temp_dir, job.name + '.o' + jobids[ix])
        log_stderr_fn = os.path.join(temp_dir, job.name + '.e' + jobids[ix])

        try:
            job_output = _zload_db(con, 'output{}'.format(uniq_id), ix)

            #print exceptions
            if isinstance(job_output, Exception):
                print("Exception encountered in job with log file:", file=sys.stderr)
                print(log_stdout_fn, file=sys.stderr)
                print(job_output, file=sys.stderr)
                print(file=sys.stderr)

        except Exception as detail:
            print("Error while unpickling output for pythongrid job {1} from table pythongrid.output{0}".format(uniq_id, ix), file=sys.stderr)
            print("This could caused by a problem with the cluster environment, imports or environment variables.", file=sys.stderr)
            print("Try running `pythongrid.py {} {} {} {}` to see if your job crashed before writing its output.".format(uniq_id, ix, job.path, temp_dir), file=sys.stderr)
            print("Check log files for more information: ", file=sys.stderr)
            print("stdout:", log_stdout_fn, file=sys.stderr)
            print("stderr:", log_stderr_fn, file=sys.stderr)
            print("Exception: {}".format(detail))
            sys.exit(2)

        job_output_list.append(job_output)

    return job_output_list


def _get_mysql_connection(verbose=False):
    '''
    Repeatedly attempt to connect to the MySQL database (with random sleeping in between attempts)
    @param verbose: Should we print status info?
    @type verbose: C{bool}
    '''
    attempts = 0
    con = None
    while attempts <= MAX_SQL_ATTEMPTS:
        try:
            if verbose:
                print("Attempting to connect to mysql database...", end=" ", file=sys.stderr)
            con = mysql.connect(db="pythongrid", host="loki.research.ets.org", user="dblanchard", passwd="Friday30")  # Yup, that's my MySQL password sitting right there.
        except mysql.Error as e:
            if verbose:
                print("FAILED", file=sys.stderr)
            con = None
            attempts += 1
            if attempts > MAX_SQL_ATTEMPTS:
                raise e
            else:
                # Randomly sleep up to two seconds
                sleep_time = random() * 2.0
                if verbose:
                    print("Sleeping for {} seconds before retrying...".format(sleep_time), file=sys.stderr)
                sleep(sleep_time)
        # It worked!
        else:
            if verbose:
                print("done", file=sys.stderr)
            break
    return con


def process_jobs(jobs, temp_dir='/scratch/', wait=True, white_list=None, quiet=True):
    """
    Take a list of jobs and process them on the cluster.

    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    @param wait: Should we wait for jobs to finish? (Should only be false if the function you're running doesn't return anything)
    @type wait: C{bool}
    @param white_list: If specified, limit nodes used to only those in list.
    @type white_list: C{list} of C{basestring}
    @param quiet: When true, do not output information about the jobs that have been submitted.
    @type quiet: C{bool}
    """

    # Create new sqlite database with pickled jobs
    con = _get_mysql_connection()

    # Generate random name for tables
    uniq_id = uuid.uuid4()

    # Create tables
    cur = con.cursor()
    cur.execute("CREATE TABLE `job{}` (`id` INT NOT NULL, `data` LONGBLOB NOT NULL)".format(uniq_id))
    cur.execute("CREATE TABLE `output{}` (`id` INT NOT NULL, `data` LONGBLOB NOT NULL)".format(uniq_id))

    # Save jobs to database
    for job_id, job in enumerate(jobs):
        _zsave_db(job, con, 'job{}'.format(uniq_id), job_id)

    # Disconnect until jobs are done because it could be a while
    con.close()

    # Submit jobs to cluster
    sids, jobids = _submit_jobs(jobs, uniq_id, white_list=white_list, temp_dir=temp_dir, quiet=quiet)

    # Reconnect and retrieve outputs
    con = _get_mysql_connection()
    cur = con.cursor()
    job_outputs = _collect_jobs(sids, jobids, jobs, con, uniq_id, temp_dir=temp_dir, wait=wait)

    # Make sure we have enough output
    assert(len(jobs) == len(job_outputs))

    # Drop tables
    cur.execute("DROP TABLE `job{}`".format(uniq_id))
    cur.execute("DROP TABLE `output{}`".format(uniq_id))

    # Close database connection
    con.close()

    return job_outputs


#####################################################################
# MapReduce Interface
#####################################################################
def pg_map(f, args_list, cleanup=True, mem_free="1G", name='pythongrid_job', num_slots=1, temp_dir='/scratch/', white_list=None, queue='nlp.q', quiet=True):
    """
    Maps a function onto the cluster.
    @note: This can only be used with picklable functions (i.e., those that are defined at the module or class level).

    @param f: The function to map on args_list
    @type f: C{function}
    @param args_list: List of arguments to pass to f
    @type args_list: C{list}
    @param cleanup: Should we remove the stdout and stderr temporary files for each job when we're done? (They are left in place if there's an error.)
    @type cleanup: C{bool}
    @param mem_free: Estimate of how much memory each job will need (for scheduling). (Not currently used, because our cluster does not have that setting enabled.)
    @type mem_free: C{basestring}
    @param name: Base name to give each job (will have a number add to end)
    @type name: C{basestring}
    @param num_slots: Number of slots each job should use.
    @type num_slots: C{int}
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    @param white_list: If specified, limit nodes used to only those in list.
    @type white_list: C{list} of C{basestring}
    @param queue: The SGE queue to use for scheduling.
    @type queue: C{basestring}
    @param quiet: When true, do not output information about the jobs that have been submitted.
    @type quiet: C{bool}
    """

    # construct jobs
    jobs = [Job(f, [args], cleanup=cleanup, mem_free=mem_free, name='{}{}'.format(name, job_num), num_slots=num_slots, queue=queue) for job_num, args in enumerate(args_list)]

    # process jobs
    job_results = process_jobs(jobs, temp_dir=temp_dir, white_list=white_list, quiet=quiet)

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
    @param con: An open connection to the database
    @type con: C{MySQLdb.Connection}
    @param table: The name of the table to retrieve data from.
    @type table: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """

    # Pickle the obj
    pickled_data = bz2.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL), 9)

    # Insert the pickled data into the database
    cur = con.cursor()
    # print("Job num type: {}\tJob num value: {}".format(type(job_num), job_num))
    cur.execute("INSERT INTO `" + table + "`(id, data) VALUES (%s, %s)", (job_num, mysql.Binary(pickled_data)))


def _zload_db(con, table, job_num):
    """
    Loads bz2-compressed pickled object from sqlite database table

    @param con: An open connection to the database
    @type con: C{MySQLdb.Connection}
    @param table: The name of the table to retrieve data from.
    @type table: C{basestring}
    @param job_num: The ID of the job this data is for.
    @type job_num: C{int}
    """
    cur = con.cursor()
    cur.execute('SELECT data FROM `{}` WHERE id={}'.format(table, job_num))
    pickled_data = cur.fetchone()[0]
    return pickle.loads(bz2.decompress(str(pickled_data)))


################################################################
#      The following code will be executed on the cluster      #
################################################################
def _run_job(uniq_id, job_num, temp_dir):
    """
    Execute the pickled job and produce pickled output (all in the SQLite3 database).

    @param uniq_id: The unique suffix for the tables corresponding to this job in the database.
    @type uniq_id: C{basestring}
    @param job_num: The index for this job's content in the job and output tables.
    @type job_num: C{int}
    @param temp_dir: Local temporary directory for storing output for an individual job.
    @type temp_dir: C{basestring}
    """
    # Connect to database
    con = _get_mysql_connection(verbose=True)

    print("Loading job...", end="", file=sys.stderr)
    sys.stderr.flush()
    job = _zload_db(con, 'job{}'.format(uniq_id), job_num)
    print("done", file=sys.stderr)

    # Disconnect while job is running, in case it takes a really long time.
    con.close()

    print("Running job...", end="", file=sys.stderr)
    sys.stderr.flush()
    job.execute()
    print("done", file=sys.stderr)

    # Reconnect to database
    con = _get_mysql_connection(verbose=True)

    print("Writing output to database for job {}...".format(job_num), end="", file=sys.stderr)
    sys.stderr.flush()
    _zsave_db(job.ret, con, 'output{}'.format(uniq_id), job_num)
    print("done", file=sys.stderr)

    #remove files
    if job.cleanup:
        log_stdout_fn = os.path.join(temp_dir, '{}.o{}'.format(job.name, job.jobid))
        log_stderr_fn = os.path.join(temp_dir, '{}.e{}'.format(job.name, job.jobid))

        try:
            os.remove(log_stdout_fn)
            os.remove(log_stderr_fn)
        except OSError:
            pass

    con.close()


def _main():
    """
    Parse the command line inputs and call _run_job
    """

    # Get command line arguments
    parser = argparse.ArgumentParser(description="This wrapper script will run a pickled Python function on some pickled data in a sqlite3 database, " +
                                                 "and write the results back to the database. You almost never want to run this yourself.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     conflict_handler='resolve')
    parser.add_argument('uniq_id', help='The unique suffix for the tables corresponding to this job in the database.')
    parser.add_argument('job_number', help='Which job number should be run. Dictates which input data is read from database and where output data is stored.', type=int)
    parser.add_argument('module_dir', help='Directory that contains module containing pickled function. This will get added to PYTHONPATH temporarily.')
    parser.add_argument('temp_dir', help='Directory that temporary output will be stored in.')
    args = parser.parse_args()

    print("Appended {} to PYTHONPATH".format(args.module_dir), file=sys.stderr)
    sys.path.append(_clean_path(args.module_dir))

    # Process the database and get job started
    _run_job(args.uniq_id, args.job_number, _clean_path(args.temp_dir))


if __name__ == "__main__":
    _main()
