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

"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import inspect
import logging
import multiprocessing
import os
import socket
import sys
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from io import open
from multiprocessing import Pool
from socket import gethostbyname, gethostname
from subprocess import Popen

import zmq

from gridmap.conf import (CHECK_FREQUENCY, CREATE_PLOTS, DEFAULT_QUEUE,
                          DRMAA_PRESENT, ERROR_MAIL_RECIPIENT,
                          ERROR_MAIL_SENDER, HEARTBEAT_FREQUENCY,
                          IDLE_THRESHOLD, MAX_IDLE_HEARTBEATS,
                          MAX_TIME_BETWEEN_HEARTBEATS, NUM_RESUBMITS,
                          SEND_ERROR_MAILS, SMTP_SERVER, USE_CHERRYPY,
                          USE_MEM_FREE)
from gridmap.data import clean_path, zdumps, zloads
from gridmap.runner import _heart_beat

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

    .. note::

       This can only be used to wrap picklable functions (i.e., those that
       are defined at the module or class level).
    """

    __slots__ = ('_f', 'args', 'jobid', 'kwlist', 'cleanup', 'ret', 'exception',
                 'num_slots', 'mem_free', 'white_list', 'path',
                 'uniq_id', 'name', 'queue', 'environment', 'working_dir',
                 'cause_of_death', 'num_resubmits', 'home_address',
                 'log_stderr_fn', 'log_stdout_fn', 'timestamp', 'host_name',
                 'heart_beat', 'track_mem', 'track_cpu')

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
        self.track_mem = []
        self.track_cpu = []
        self.heart_beat = None
        self.exception = None
        self.host_name = ''
        self.timestamp = None
        self.log_stdout_fn = ''
        self.log_stderr_fn = ''
        self.home_address = ''
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
        self.environment = {}
        for env_var, value in os.environ.items():
            try:
                if not isinstance(env_var, bytes):
                    env_var = env_var.encode()
                if not isinstance(value, bytes):
                    value = value.encode()
            except UnicodeEncodeError:
                logger = logging.getLogger(__name__)
                logger.warning('Skipping non-ASCII environment variable.')
            else:
                self.environment[env_var] = value
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
            ret += " -N {}".format(self.name)
        if self.mem_free and USE_MEM_FREE:
            ret += " -l mem_free={}".format(self.mem_free)
        if self.num_slots and self.num_slots > 1:
            ret += " -pe smp {}".format(self.num_slots)
        if self.white_list:
            ret += " -l h={}".format('|'.join(self.white_list))
        if self.queue:
            ret += " -q {}".format(self.queue)

        return ret


###############################
# Job Submission and Monitoring
###############################

class JobMonitor(object):
    """
    Job monitor that communicates with other nodes via 0MQ.
    """
    def __init__(self, temp_dir='/scratch'):
        """
        set up socket
        """
        logger = logging.getLogger(__name__)

        context = zmq.Context()
        self.temp_dir = temp_dir
        self.socket = context.socket(zmq.REP)

        self.host_name = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.host_name)
        self.interface = "tcp://%s" % (self.ip_address)

        # bind to random port and remember it
        self.port = self.socket.bind_to_random_port(self.interface)
        self.home_address = "%s:%i" % (self.interface, self.port)

        logger.info("setting up connection on %s", self.home_address)

        self.started_cherrypy = not USE_CHERRYPY

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
        logger = logging.getLogger(__name__)

        # save list of jobs
        self.jobs = jobs

        # keep track of DRMAA session_id (for resubmissions)
        self.session_id = session_id

        # save useful mapping
        self.jobid_to_job = {job.jobid: job for job in jobs}

        # start web interface
        if not self.started_cherrypy:
            job_paths = list({job.path for job in self.jobs})
            logger.info("starting web interface")
            # TODO: Check that it isn't already running
            cherrypy_proc = Popen([sys.executable, "-m", "gridmap.web"] +
                                  job_paths)
            self.started_cherrypy = True
        else:
            cherrypy_proc = None

        # determines in which interval to check if jobs are alive
        local_heart = multiprocessing.Process(target=_heart_beat,
                                              args=(-1, self.home_address, -1,
                                                    "", CHECK_FREQUENCY))
        local_heart.start()
        logger.info("Starting ZMQ event loop")
        # main loop
        while not self.all_jobs_done():
            logger.debug('Waiting for message')
            msg_str = self.socket.recv()
            msg = zloads(msg_str)
            logger.debug('Received message: {}'.format(msg))
            return_msg = ""

            job_id = msg["job_id"]

            # only if its not the local beat
            if job_id != -1:
                logger.debug('Received message: %s', msg)

                try:
                    job = self.jobid_to_job[job_id]
                except KeyError:
                    logger.error(('Received message from unknown job with ID ' +
                                  '%s. Known job IDs are: %s'),
                                 job_id,
                                 list(self.jobid_to_job.keys()))
                    continue

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
                    # is assigned in submission process and not written back
                    # server-side
                    job.timestamp = datetime.now()

                if msg["command"] == "heart_beat":
                    job.heart_beat = msg["data"]

                    # keep track of mem and cpu
                    try:
                        job.track_mem.append(job.heart_beat["memory"])
                        job.track_cpu.append(job.heart_beat["cpu_load"])
                    except (ValueError, TypeError):
                        logger.error("error decoding heart-beat", exc_info=True)
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
            logger.debug('Sending reply: %s', return_msg)
            self.socket.send(zdumps(return_msg))

        # Kill child processes that we don't need anymore
        local_heart.terminate()
        if cherrypy_proc is not None:
            cherrypy_proc.terminate()

    def check_if_alive(self):
        """
        check if jobs are alive and determine cause of death if not
        """
        logger = logging.getLogger(__name__)
        for job in self.jobs:

            # noting was returned yet
            if job.ret is None:

                # exclude first-timers
                if job.timestamp is not None:

                    # check heart-beats if there was a long delay
                    current_time = datetime.now()
                    time_delta = current_time - job.timestamp
                    if time_delta.seconds > MAX_TIME_BETWEEN_HEARTBEATS:
                        logger.error("job died for unknown reason")
                        job.cause_of_death = "unknown"
                    elif (len(job.track_cpu) > MAX_IDLE_HEARTBEATS and
                          all(cpu_load <= IDLE_THRESHOLD and state == 'S'
                              for cpu_load, state in
                              job.track_cpu[-MAX_IDLE_HEARTBEATS:])):
                        logger.error('Job stalled for unknown reason.')
                        job.cause_of_death = 'stalled'

            # could have been an exception, we check right away
            elif isinstance(job.ret, Exception):
                logger.error("Job encountered exception; will not resubmit.")
                job.cause_of_death = "exception"
                send_error_mail(job)
                job.ret = "Job dead. Exception: {}".format(job.ret)

            # attempt to resubmit
            if job.cause_of_death:
                logger.info("creating error report")

                # send report
                send_error_mail(job)

                # try to resubmit
                old_id = job.jobid
                handle_resubmit(self.session_id, job, temp_dir=self.temp_dir)
                if job.jobid is None:
                    logger.error("giving up on job")
                    job.ret = "job dead"
                # Update job ID if successfully resubmitted
                else:
                    del self.jobid_to_job[old_id]
                    self.jobid_to_job[job.jobid] = job

                # break out of loop to avoid too long delay
                break

    def all_jobs_done(self):
        """
        checks for all jobs if they are done
        """
        logger = logging.getLogger(__name__)
        if logger.getEffectiveLevel() == logging.DEBUG:
            num_jobs = len(self.jobs)
            num_completed = sum((job.ret is not None and
                                 not isinstance(job.ret, Exception))
                                for job in self.jobs)
            logger.debug('%i out of %i jobs completed', num_completed,
                         num_jobs)

        # exceptions will be handled in check_if_alive
        return all((job.ret is not None and not isinstance(job.ret, Exception))
                   for job in self.jobs)


def send_error_mail(job):
    """
    send out diagnostic email
    """
    logger = logging.getLogger(__name__)

    # create message
    msg = MIMEMultipart()
    msg["subject"] = "GridMap error {}".format(job.name)
    msg["From"] = ERROR_MAIL_SENDER
    msg["To"] = ERROR_MAIL_RECIPIENT

    # compose error message
    body_text = ""
    body_text += "job {}\n".format(job.name)
    body_text += "last timestamp: {}\n".format(job.timestamp)
    body_text += "num_resubmits: {}\n".format(job.num_resubmits)
    body_text += "cause_of_death: {}\n".format(job.cause_of_death)

    if job.heart_beat:
        body_text += "last memory usage: {}\n".format(job.heart_beat["memory"])
        body_text += "last cpu load: {}\n".format(job.heart_beat["cpu_load"][0])
        body_text += ("last process state: " +
                      "{}\n\n").format(job.heart_beat["cpu_load"][1])

    body_text += "host: {}\n\n".format(job.host_name)

    if isinstance(job.ret, Exception):
        body_text += "job encountered exception: {}\n".format(job.ret)
        body_text += "stacktrace: {}\n\n".format(job.exception)

    logger.info('Email body: %s', body_text)

    body_msg = MIMEText(body_text)
    msg.attach(body_msg)

    # attach log file
    if job.heart_beat and os.path.exists(job.heart_beat["log_file"]):
        log_file_fn = job.heart_beat['log_file']
        with open(log_file_fn, "rb") as log_file:
            log_file_attachement = MIMEText(log_file.read())
        log_file_attachement.add_header('Content-Disposition', 'attachment',
                                        filename='{}_log.txt'.format(job.jobid))
        msg.attach(log_file_attachement)

    # if matplotlib is installed
    if CREATE_PLOTS:
        import matplotlib
        matplotlib.use('AGG')
        import matplotlib.pyplot as plt
        #TODO: plot to cstring directly (some code is there)
        #imgData = cStringIO.StringIO()
        #plt.savefig(imgData, format='png')

        # rewind the data
        #imgData.seek(0)
        #plt.savefig(imgData, format="png")

        time = [HEARTBEAT_FREQUENCY * i for i in range(len(job.track_mem))]

        # attack mem plot
        img_mem_fn = os.path.join('/tmp', "{}_mem.png".format(job.jobid))
        plt.figure(1)
        plt.plot(time, job.track_mem, "-o")
        plt.xlabel("time (s)")
        plt.ylabel("memory usage")
        plt.savefig(img_mem_fn)
        plt.close()
        with open(img_mem_fn, "rb") as img_mem:
            img_data = img_mem.read()
        img_mem_attachement = MIMEImage(img_data)
        img_mem_attachement.add_header('Content-Disposition', 'attachment',
                                       filename=os.path.basename(img_mem_fn))
        msg.attach(img_mem_attachement)

        # attach cpu plot
        img_cpu_fn = os.path.join("/tmp", "{}_cpu.png".format(job.jobid))
        plt.figure(2)
        plt.plot(time, [cpu_load for cpu_load, _ in job.track_cpu], "-o")
        plt.xlabel("time (s)")
        plt.ylabel("cpu load")
        plt.savefig(img_cpu_fn)
        plt.close()
        with open(img_cpu_fn, "rb") as img_cpu:
            img_data = img_cpu.read()
        img_cpu_attachement = MIMEImage(img_data)
        img_cpu_attachement.add_header('Content-Disposition', 'attachment',
                                       filename=os.path.basename(img_cpu_fn))
        msg.attach(img_cpu_attachement)

    if SEND_ERROR_MAILS:
        import smtplib
        try:
            s = smtplib.SMTP(SMTP_SERVER)
        except smtplib.SMTPConnectError:
            logger.error('Failed to connect to SMTP server to send error ' +
                         'email.', exc_info=True)
        else:
            s.sendmail(ERROR_MAIL_SENDER, ERROR_MAIL_RECIPIENT, msg.as_string())
            # Clean up plot temporary files
            if CREATE_PLOTS:
                os.unlink(img_cpu_fn)
                os.unlink(img_mem_fn)
            s.quit()


def handle_resubmit(session_id, job, temp_dir='/scratch/'):
    """
    heuristic to determine if the job should be resubmitted

    side-effect:
    job.num_resubmits incremented
    """
    # reset some fields
    job.timestamp = None
    job.heart_beat = None

    if job.num_resubmits < NUM_RESUBMITS:
        logger = logging.getLogger(__name__)
        logger.warning("Looks like job died an unnatural death, resubmitting" +
                       "(previous resubmits = %i)", job.num_resubmits)

        # remove node from white_list
        node_name = '{}@{}'.format(job.queue, job.host_name)
        if job.white_list:
            job.white_list.remove(node_name)

        # increment number of resubmits
        job.num_resubmits += 1
        job.cause_of_death = ""

        return _resubmit(session_id, job, temp_dir)
    else:
        return None


def _execute(job):
    """
    Cannot pickle method instances, so fake a function.
    Used by _process_jobs_locally
    """
    job.execute()


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
    :param home_address: Full address (including IP and port) of JobMonitor on
                         submitting host. Running jobs will communicate with the
                         parent process at that address via ZMQ.
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

    :returns: Session ID, list of job IDs
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

    :returns: Job ID
    """

    jt = session.createJobTemplate()
    logger = logging.getLogger(__name__)
    # logger.debug('{}'.format(job.environment))
    jt.jobEnvironment = job.environment

    # Run module using python -m to avoid ImportErrors when unpickling jobs
    jt.remoteCommand = sys.executable
    ip = gethostbyname(gethostname())
    jt.args = ['-m', 'gridmap.runner', '{}'.format(job.home_address), job.path]
    jt.nativeSpecification = job.native_specification
    jt.workingDirectory = job.working_dir
    jt.outputPath = ":{}".format(temp_dir)
    jt.errorPath = ":{}".format(temp_dir)

    # Create temp directory if necessary
    if not os.path.exists(temp_dir):
        try:
            os.makedirs(temp_dir)
        except OSError:
            logger.warning(("Failed to create temporary directory " +
                            "{}.  Your jobs may not start " +
                            "correctly.").format(temp_dir))

    jobid = session.runJob(jt)

    # set job fields that depend on the jobid assigned by grid engine
    job.jobid = jobid
    job.log_stdout_fn = os.path.join(temp_dir, '{}.o{}'.format(job.name, jobid))
    job.log_stderr_fn = os.path.join(temp_dir, '{}.e{}'.format(job.name, jobid))

    if not quiet:
        print('Your job {} has been submitted with id {}'.format(job.name,
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

    :returns: List of Job results
    """
    if (not local and not DRMAA_PRESENT):
        logger = logging.getLogger(__name__)
        logger.warning('Could not import drmaa. Processing jobs locally.')
        local = False

    if not local:
        # initialize monitor to get port number
        monitor = JobMonitor(temp_dir=temp_dir)

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


def _resubmit(session_id, job, temp_dir):
    """
    Resubmit a failed job.

    :returns: ID of new job
    """
    logger = logging.getLogger(__name__)
    logger.info("starting resubmission process")

    if DRMAA_PRESENT:
        # append to session
        with Session(session_id) as session:
            # try to kill off old job
            try:
                session.control(job.jobid, JobControlAction.TERMINATE)
                logger.info("zombie job killed")
            except Exception:
                logger.error("Could not kill job with SGE id %s", job.jobid,
                             exc_info=True)
            # create new job
            _append_job_to_session(session, job, temp_dir=temp_dir)
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

    .. note::

       This can only be used with picklable functions (i.e., those that are
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

    :returns: List of Job results
    """

    # construct jobs
    jobs = [Job(f, [args] if not isinstance(args, list) else args,
                cleanup=cleanup, mem_free=mem_free,
                name='{}{}'.format(name, job_num), num_slots=num_slots,
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
    .. deprecated:: 0.9
       This function has been renamed grid_map.

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

    :returns: List of Job results
    """
    return grid_map(f, args_list, cleanup=cleanup, mem_free=mem_free, name=name,
                    num_slots=num_slots, temp_dir=temp_dir,
                    white_list=white_list, queue=queue, quiet=quiet)
