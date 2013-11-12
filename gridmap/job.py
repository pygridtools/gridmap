# -*- coding: utf-8 -*-

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2013 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2013 ETS

# This file is part of GridMap.

# GridMap is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# GridMap is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with GridMap.  If not, see <http://www.gnu.org/licenses/>.

"""
This module provides wrappers that simplify submission and collection of jobs,
in a more 'pythonic' fashion.

We use pyZMQ to provide a heart beat feature that allows close monitoring
of submitted jobs and take appropriate action in case of failure.

:author: Christian Widmer
:author: Cheng Soon Ong
:author: Dan Blanchard (dblanchard@ets.org)

:var REDIS_DB: The index of the database to select on the Redis server; can be
               overriden by setting the GRID_MAP_REDIS_DB environment variable.
:var REDIS_PORT: The port of the Redis server to use; can be overriden by
                 setting the GRID_MAP_REDIS_PORT environment variable.
:var USE_MEM_FREE: Does your cluster support specifying how much memory a job
                   will use via mem_free? Can be overriden by setting the
                   GRID_MAP_USE_MEM_FREE environment variable.
:var DEFAULT_QUEUE: The default job scheduling queue to use; can be overriden
                    via the GRID_MAP_DEFAULT_QUEUE environment variable.
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import inspect
import logging
import os
import sys
import traceback
from multiprocessing import Pool
from socket import gethostbyname, gethostname

from gridmap.conf import DEFAULT_QUEUE, DRMAA_PRESENT, USE_MEM_FREE
from gridmap.data import clean_path
from gridmap.monitor import JobMonitor

if DRMAA_PRESENT:
    from drmaa import JobControlAction, Session

# Python 2.x backward compatibility
if sys.version_info < (3, 0):
    range = xrange


class JobException(Exception):
    '''
    New exception type for when one of the jobs crashed.
    '''
    pass


class Job(object):
    """
    Central entity that wraps a function and its data. Basically, a job consists
    of a function, its argument list, its keyword list and a field "ret" which
    is filled, when the execute method gets called.

    :note: This can only be used to wrap picklable functions (i.e., those that
    are defined at the module or class level).
    """

    __slots__ = ('_f', 'args', 'jobid', 'kwlist', 'cleanup', 'ret', 'exception',
                 'num_slots', 'mem_free', 'white_list', 'path',
                 'uniq_id', 'name', 'queue', 'environment', 'working_dir',
                 'cause_of_death', 'num_resubmits')

    def __init__(self, f, args, kwlist=None, cleanup=True, mem_free="1G",
                 name='gridmap_job', num_slots=1, queue=DEFAULT_QUEUE):
        """
        Initializes a new Job.

        :param f: a function, which should be executed.
        :type f: function
        :param args: argument list of function f
        :type args: list
        :param kwlist: dictionary of keyword arguments for f
        :type kwlist: dict
        :param cleanup: flag that determines the cleanup of input and log file
        :type cleanup: boolean
        :param mem_free: Estimate of how much memory this job will need (for
                         scheduling)
        :type mem_free: str
        :param name: Name to give this job
        :type name: str
        :param num_slots: Number of slots this job should use.
        :type num_slots: int
        :param queue: SGE queue to schedule job on.
        :type queue: str
        """
        self.num_resubmits = 0
        self.cause_of_death = ''
        self.path = None
        self._f = None
        self.function = f
        self.args = args
        self.jobid = -1
        self.kwlist = kwlist if kwlist is not None else {}
        self.cleanup = cleanup
        self.ret = None
        self.num_slots = num_slots
        self.mem_free = mem_free
        self.white_list = []
        self.name = name.replace(' ', '_')
        self.queue = queue
        # Save copy of environment variables
        self.environment = {env_var: value for env_var, value in
                            os.environ.items()}
        self.working_dir = clean_path(os.getcwd())

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
            self.path = clean_path(os.path.dirname(os.path.abspath(
                inspect.getsourcefile(f))))
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
        except Exception as exception:
            self.ret = exception
            traceback.print_exc()
        del self.args
        del self.kwlist

    @property
    def native_specification(self):
        """
        define python-style getter
        """

        ret = "-shell yes -b yes"

        if self.name:
            ret += " -N {0}".format(self.name)
        if self.mem_free and USE_MEM_FREE:
            ret += " -l mem_free={0}".format(self.mem_free)
        if self.num_slots and self.num_slots > 1:
            ret += " -pe smp {0}".format(self.num_slots)
        if self.white_list:
            ret += " -l h={0}".format('|'.join(self.white_list))
        if self.queue:
            ret += " -q {0}".format(self.queue)

        return ret


def _execute(job):
    """Cannot pickle method instances, so fake a function.
    Used by _process_jobs_locally"""

    return job.f(*job.args, **job.kwlist)


def _process_jobs_locally(jobs, max_processes=1):
    """
    Local execution using the package multiprocessing, if present

    :param jobs: jobs to be executed
    :type jobs: list of Job
    :param max_processes: maximal number of processes
    :type max_processes: int

    :return: list of jobs, each with return in job.ret
    :rtype: list of Job
    """
    logger = logging.getLogger(__name__)
    logger.info("using %i processes", max_processes)

    if max_processes == 1:
        # perform sequential computation
        for job in jobs:
            job.execute()
    else:
        pool = Pool(max_processes)
        result = pool.map(_execute, jobs)
        for ix, job in enumerate(jobs):
            job.ret = result[ix]
        pool.close()
        pool.join()

    return jobs


def _submit_jobs(jobs, home_address, temp_dir='/scratch', white_list=None,
                 quiet=True):
    """
    Method used to send a list of jobs onto the cluster.
    :param jobs: list of jobs to be executed
    :type jobs: list of `Job`
    :param home_address: IP address of submitting machine. Running jobs will
                         communicate with the parent process at that IP via ZMQ.
    :type home_address: str
    :param temp_dir: Local temporary directory for storing output for an
                     individual job.
    :type temp_dir: str
    :param white_list: List of acceptable nodes to use for scheduling job. If
                       None, all are used.
    :type white_list: list of str
    :param quiet: When true, do not output information about the jobs that have
                  been submitted.
    :type quiet: bool
    """
    with Session() as session:
        jobids = []
        for job in jobs:
            # set job white list
            job.white_list = white_list

            # remember address of submission host
            job.home_address = home_address

            # append jobs
            jobid = _append_job_to_session(session, job, temp_dir=temp_dir,
                                           quiet=quiet)
            jobids.append(jobid)

        sid = session.contact
    return (sid, jobids)


def _append_job_to_session(session, job, temp_dir='/scratch/', quiet=True):
    """
    For an active session, append new job based on information stored in job
    object. Also sets job.job_id to the ID of the job on the grid.

    :param session: The current DRMAA session with the grid engine.
    :type session: Session
    :param job: The Job to add to the queue.
    :type job: `Job`
    :param temp_dir: Local temporary directory for storing output for an
                    individual job.
    :type temp_dir: str
    :param quiet: When true, do not output information about the jobs that have
                  been submitted.
    :type quiet: bool
    """

    jt = session.createJobTemplate()

    logging.debug('{0}'.format(job.environment))
    jt.jobEnvironment = job.environment

    # Run module using python -m to avoid ImportErrors when unpickling jobs
    jt.remoteCommand = sys.executable
    ip = gethostbyname(gethostname())
    jt.args = ['-m', 'gridmap.runner', '{0}'.format(job.name), '{0}'.format(ip),
               job.path]
    jt.nativeSpecification = job.native_specification
    jt.workingDirectory = job.working_dir
    jt.outputPath = ":{0}".format(temp_dir)
    jt.errorPath = ":{0}".format(temp_dir)

    # Create temp directory if necessary
    if not os.path.exists(temp_dir):
        try:
            os.makedirs(temp_dir)
        except OSError:
            logging.warning(("Failed to create temporary directory " +
                             "{0}.  Your jobs may not start " +
                             "correctly.").format(temp_dir))

    jobid = session.runJob(jt)

    # set job fields that depend on the jobid assigned by grid engine
    job.jobid = jobid

    if not quiet:
        print('Your job {0} has been submitted with id {1}'.format(job.name,
                                                                   jobid),
              file=sys.stderr)

    session.deleteJobTemplate(jt)

    return jobid


def process_jobs(jobs, temp_dir='/scratch/', white_list=None, quiet=True,
                 max_processes=1, local=False):
    """
    Take a list of jobs and process them on the cluster.

    :param jobs: Jobs to run.
    :type jobs: list of Job
    :param temp_dir: Local temporary directory for storing output for an
                     individual job.
    :type temp_dir: str
    :param white_list: If specified, limit nodes used to only those in list.
    :type white_list: list of str
    :param quiet: When true, do not output information about the jobs that have
                  been submitted.
    :type quiet: bool
    :param max_processes: The maximum number of concurrent processes to use if
                          processing jobs locally.
    :type max_processes: int
    :param local: Should we execute the jobs locally in separate processes
                  instead of on the the cluster?
    :type local: bool
    """
    if (not local and not DRMAA_PRESENT):
        logger = logging.getLogger(__name__)
        logger.warning('Could not import drmaa. Processing jobs locally.')
        local = False

    if not local:
        # initialize monitor to get port number
        monitor = JobMonitor()

        # get interface and port
        home_address = monitor.home_address

        # jobid field is attached to each job object
        sid = _submit_jobs(jobs, home_address, temp_dir=temp_dir,
                           white_list=white_list, quiet=quiet)[0]

        # handling of inputs, outputs and heartbeats
        monitor.check(sid, jobs)
    else:
        _process_jobs_locally(jobs, max_processes=max_processes)

    return [job.ret for job in jobs]


def _resubmit(session_id, job):
    """
    Resubmit a failed job.
    """
    logger = logging.getLogger(__name__)
    logger.info("starting resubmission process")

    if DRMAA_PRESENT:
        # append to session
        with Session(session_id) as session:
            # try to kill off old job
            try:
                # TODO: ask SGE more questions about job status etc
                # TODO: write unit test for this
                session.control(job.jobid, JobControlAction.TERMINATE)
                logger.info("zombie job killed")
            except Exception:
                logger.error("Could not kill job with SGE id %s", job.jobid,
                             exc_info=True)
            # create new job
            _append_job_to_session(session, job)
    else:
        logger.error("Could not restart job because we're in local mode.")


#####################
# MapReduce Interface
#####################
def grid_map(f, args_list, cleanup=True, mem_free="1G", name='gridmap_job',
             num_slots=1, temp_dir='/scratch/', white_list=None,
             queue=DEFAULT_QUEUE, quiet=True):
    """
    Maps a function onto the cluster.
    :note: This can only be used with picklable functions (i.e., those that are
           defined at the module or class level).

    :param f: The function to map on args_list
    :type f: function
    :param args_list: List of arguments to pass to f
    :type args_list: list
    :param cleanup: Should we remove the stdout and stderr temporary files for
                    each job when we're done? (They are left in place if there's
                    an error.)
    :type cleanup: bool
    :param mem_free: Estimate of how much memory each job will need (for
                     scheduling). (Not currently used, because our cluster does
                     not have that setting enabled.)
    :type mem_free: str
    :param name: Base name to give each job (will have a number add to end)
    :type name: str
    :param num_slots: Number of slots each job should use.
    :type num_slots: int
    :param temp_dir: Local temporary directory for storing output for an
                     individual job.
    :type temp_dir: str
    :param white_list: If specified, limit nodes used to only those in list.
    :type white_list: list of str
    :param queue: The SGE queue to use for scheduling.
    :type queue: str
    :param quiet: When true, do not output information about the jobs that have
                  been submitted.
    :type quiet: bool
    """

    # construct jobs
    jobs = [Job(f, [args] if not isinstance(args, list) else args,
                cleanup=cleanup, mem_free=mem_free,
                name='{0}{1}'.format(name, job_num), num_slots=num_slots,
                queue=queue)
            for job_num, args in enumerate(args_list)]

    # process jobs
    job_results = process_jobs(jobs, temp_dir=temp_dir, white_list=white_list,
                               quiet=quiet)

    return job_results


def pg_map(f, args_list, cleanup=True, mem_free="1G", name='gridmap_job',
           num_slots=1, temp_dir='/scratch/', white_list=None,
           queue=DEFAULT_QUEUE, quiet=True):
    """
    :deprecated: This function has been renamed grid_map.

    :param f: The function to map on args_list
    :type f: function
    :param args_list: List of arguments to pass to f
    :type args_list: list
    :param cleanup: Should we remove the stdout and stderr temporary files for
                    each job when we're done? (They are left in place if there's
                    an error.)
    :type cleanup: bool
    :param mem_free: Estimate of how much memory each job will need (for
                     scheduling). (Not currently used, because our cluster does
                     not have that setting enabled.)
    :type mem_free: str
    :param name: Base name to give each job (will have a number add to end)
    :type name: str
    :param num_slots: Number of slots each job should use.
    :type num_slots: int
    :param temp_dir: Local temporary directory for storing output for an
                     individual job.
    :type temp_dir: str
    :param white_list: If specified, limit nodes used to only those in list.
    :type white_list: list of str
    :param queue: The SGE queue to use for scheduling.
    :type queue: str
    :param quiet: When true, do not output information about the jobs that have
                  been submitted.
    :type quiet: bool
    """
    return grid_map(f, args_list, cleanup=cleanup, mem_free=mem_free, name=name,
                    num_slots=num_slots, temp_dir=temp_dir,
                    white_list=white_list, queue=queue, quiet=quiet)
