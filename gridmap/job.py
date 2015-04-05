# -*- coding: utf-8 -*-

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2014 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2014 ETS

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
import smtplib
import sys
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from io import open
from importlib import import_module
from multiprocessing import Pool
from socket import gethostname, gethostbyname, getaddrinfo, getfqdn
from smtplib import (SMTPRecipientsRefused, SMTPHeloError, SMTPSenderRefused,
                     SMTPDataError)

import zmq

from gridmap.conf import (CHECK_FREQUENCY, CREATE_PLOTS, DEFAULT_QUEUE,
                          DRMAA_PRESENT, ERROR_MAIL_RECIPIENT,
                          ERROR_MAIL_SENDER, HEARTBEAT_FREQUENCY,
                          IDLE_THRESHOLD, MAX_IDLE_HEARTBEATS,
                          MAX_TIME_BETWEEN_HEARTBEATS, NUM_RESUBMITS,
                          SEND_ERROR_MAIL, SMTP_SERVER, USE_MEM_FREE)
from gridmap.data import zdumps, zloads
from gridmap.runner import _heart_beat

if DRMAA_PRESENT:
    from drmaa import (ExitTimeoutException, InvalidJobException,
                       JobControlAction, JOB_IDS_SESSION_ALL, Session,
                       TIMEOUT_NO_WAIT)

# Python 2.x backward compatibility
if sys.version_info < (3, 0):
    range = xrange

# Setup back-end if we're using matplotlib
if CREATE_PLOTS:
    import matplotlib
    matplotlib.use('AGG')
    import matplotlib.pyplot as plt

# Placeholder string, since a job could potentially return None on purpose
_JOB_NOT_FINISHED = '*@#%$*@#___GRIDMAP___NOT___DONE___@#%**#*$&*%'


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

    __slots__ = ('_f', 'args', 'id', 'kwlist', 'cleanup', 'ret', 'traceback',
                 'num_slots', 'mem_free', 'white_list', 'path', 'uniq_id',
                 'name', 'queue', 'environment', 'working_dir',
                 'cause_of_death', 'num_resubmits', 'home_address',
                 'log_stderr_fn', 'log_stdout_fn', 'timestamp', 'host_name',
                 'heart_beat', 'track_mem', 'track_cpu', 'engine')

    def __init__(self, f, args, kwlist=None, cleanup=True, mem_free="1G",
                 name='gridmap_job', num_slots=1, queue=DEFAULT_QUEUE, engine="SGE"):
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

        :param engine: Indicates compatability with a grid engine. Either SGE or TORQUE / PBS
        :type engine: str

        """
        self.track_mem = []
        self.track_cpu = []
        self.heart_beat = None
        self.traceback = None
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
        self.id = -1
        self.kwlist = kwlist if kwlist is not None else {}
        self.cleanup = cleanup
        self.ret = _JOB_NOT_FINISHED
        self.num_slots = num_slots
        self.mem_free = mem_free
        self.white_list = []
        self.name = name.replace(' ', '_')
        self.queue = queue
        self.engine = engine
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
        self.working_dir = os.getcwd()

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
            self.path = os.path.dirname(os.path.abspath(
                inspect.getsourcefile(f)))
        except TypeError:
            self.path = ''

        # if module is not __main__, all is good
        if m.__name__ != "__main__":
            self._f = f

        else:

            # determine real module name
            mn = os.path.splitext(os.path.basename(m.__file__))[0]

            # make sure module is present
            import_module(mn)

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
            self.traceback = traceback.format_exc()
            traceback.print_exc()

    @property
    def native_specification(self):
        """
        define python-style getter
        """
        pbs = (self.engine == "TORQUE" or self.engine == "PBS")
        sge = (self.engine == "SGE")

        ret = ""

        if sge:
            ret = "-shell yes -b yes"

        if self.num_slots and self.num_slots > 1:
            if sge:
                ret += " -pe smp {}".format(self.num_slots)
            if pbs:
                ret += " -l nodes=1:ppn={}".format(self.num_slots)

        if self.mem_free and USE_MEM_FREE:
            if sge:
                ret += " -l mem_free={}".format(self.mem_free)
            if pbs:
                ret += " -l mem={}".format(self.mem_free)

        if self.white_list:
            if sge:
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
        self.logger = logging.getLogger(__name__)

        context = zmq.Context()
        self.temp_dir = temp_dir
        self.socket = context.socket(zmq.REP)

        self.host_name = gethostname()
        self.ip_address = gethostbyname(self.host_name)

        for _, _, _, _, (ip, _) in getaddrinfo(getfqdn(), 0):
            if ip != '127.0.0.1':
                self.ip_address = ip
                self.interface = "tcp://%s" % (self.ip_address)
                break
        else:
            self.logger.warning('IP address for JobMonitor server is '
                                '127.0.0.1.  Runners on other machines will be'
                                ' unable to connect.')
            self.ip_address = '127.0.0.1'

        self.interface = "tcp://%s" % (self.ip_address)

        # bind to random port and remember it
        self.port = self.socket.bind_to_random_port(self.interface)
        self.home_address = "%s:%i" % (self.interface, self.port)

        self.logger.info("Setting up JobMonitor on %s", self.home_address)

        # uninitialized field (set in check method)
        self.jobs = []
        self.ids = []
        self.session_id = None
        self.id_to_job = {}

    def __enter__(self):
        '''
        Enable JobMonitor to be used as a context manager.
        '''
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        '''
        Gracefully handle exceptions by terminating all jobs, and closing
        sockets.
        '''
        # Always close socket
        self.socket.close()

        # Clean up if we have a valid session
        if self.session_id is not None:
            with Session(self.session_id) as session:
                # If we encounter an exception, kill all jobs
                if exc_type is not None:
                    self.logger.info('Encountered %s, so killing all jobs.',
                                     exc_type.__name__)
                    # try to kill off all old jobs
                    try:
                        session.control(JOB_IDS_SESSION_ALL,
                                        JobControlAction.TERMINATE)
                    except InvalidJobException:
                        self.logger.debug("Could not kill all jobs for " +
                                          "session.", exc_info=True)

                # Get rid of job info to prevent memory leak
                try:
                    session.synchronize([JOB_IDS_SESSION_ALL], TIMEOUT_NO_WAIT,
                                        dispose=True)
                except ExitTimeoutException:
                    pass

    def check(self, session_id, jobs):
        """
        serves input and output data
        """
        # save list of jobs
        self.jobs = jobs
        self.id_to_job = {job.id: job for job in self.jobs}

        # keep track of DRMAA session_id (for resubmissions)
        self.session_id = session_id

        # determines in which interval to check if jobs are alive
        self.logger.debug('Starting local hearbeat')
        local_heart = multiprocessing.Process(target=_heart_beat,
                                              args=(-1, self.home_address, -1,
                                                    "", CHECK_FREQUENCY))
        local_heart.start()
        try:
            self.logger.debug("Starting ZMQ event loop")
            # main loop
            while not self.all_jobs_done():
                self.logger.debug('Waiting for message')
                msg_str = self.socket.recv()
                msg = zloads(msg_str)
                self.logger.debug('Received message: %s', msg)
                return_msg = ""

                job_id = msg["job_id"]

                # only if its not the local beat
                if job_id != -1:
                    # If message is from a valid job, process that message
                    if job_id in self.id_to_job:
                        job = self.id_to_job[job_id]

                        if msg["command"] == "fetch_input":
                            return_msg = self.id_to_job[job_id]
                            job.timestamp = datetime.now()
                            self.logger.debug("Received input request from %s",
                                              job_id)

                        if msg["command"] == "store_output":
                            # be nice
                            return_msg = "thanks"

                            # store tmp job object
                            if isinstance(msg["data"], Job):
                                tmp_job = msg["data"]
                                # copy relevant fields
                                job.ret = tmp_job.ret
                                job.traceback = tmp_job.traceback
                                self.logger.info("Received output from %s",
                                                  job_id)
                            # Returned exception instead of job, so store that
                            elif isinstance(msg["data"], tuple):
                                job.ret, job.traceback = msg["data"]
                                self.logger.info("Received exception from %s",
                                                  job_id)
                            else:
                                self.logger.error(("Received message with " +
                                                   "invalid data: %s"), msg)
                                job.ret = msg["data"]
                            job.timestamp = datetime.now()

                        if msg["command"] == "heart_beat":
                            job.heart_beat = msg["data"]

                            # keep track of mem and cpu
                            try:
                                job.track_mem.append(job.heart_beat["memory"])
                                job.track_cpu.append(job.heart_beat["cpu_load"])
                            except (ValueError, TypeError):
                                self.logger.error("Error decoding heart-beat",
                                                  exc_info=True)
                            return_msg = "all good"
                            job.timestamp = datetime.now()

                        if msg["command"] == "get_job":
                            # serve job for display
                            return_msg = job
                        else:
                            # update host name
                            job.host_name = msg["host_name"]
                    # If this is an unknown job, report it and reply
                    else:
                        self.logger.error(('Received message from unknown job' +
                                           ' with ID %s. Known job IDs are: ' +
                                           '%s'), job_id,
                                          list(self.id_to_job.keys()))
                        return_msg = 'thanks, but no thanks'
                else:
                    # run check
                    self.check_if_alive()

                    if msg["command"] == "get_jobs":
                        # serve list of jobs for display
                        return_msg = self.jobs

                # send back compressed response
                self.logger.debug('Sending reply: %s', return_msg)
                self.socket.send(zdumps(return_msg))
        finally:
            # Kill child processes that we don't need anymore
            local_heart.terminate()

    def check_if_alive(self):
        """
        check if jobs are alive and determine cause of death if not
        """
        self.logger.debug('Checking if jobs are alive')
        for job in self.jobs:

            # noting was returned yet
            if job.ret == _JOB_NOT_FINISHED:

                # exclude first-timers
                if job.timestamp is not None:
                    # check heart-beats if there was a long delay
                    current_time = datetime.now()
                    time_delta = current_time - job.timestamp
                    if time_delta.seconds > MAX_TIME_BETWEEN_HEARTBEATS:
                        self.logger.debug("It has been %s seconds since we " +
                                          "received a message from job %s",
                                          time_delta.seconds, job.id)
                        self.logger.error("Job died for unknown reason")
                        job.cause_of_death = "unknown"
                    elif (len(job.track_cpu) > MAX_IDLE_HEARTBEATS and
                          all(cpu_load <= IDLE_THRESHOLD and not running
                              for cpu_load, running in
                              job.track_cpu[-MAX_IDLE_HEARTBEATS:])):
                        self.logger.error('Job stalled for unknown reason.')
                        job.cause_of_death = 'stalled'

            # could have been an exception, we check right away
            elif isinstance(job.ret, Exception):
                job.cause_of_death = 'exception'

                # Send error email, in addition to raising and logging exception
                if SEND_ERROR_MAIL:
                    send_error_mail(job)

                # Format traceback much like joblib does
                self.logger.error("-" * 80)
                self.logger.error("GridMap job traceback for %s:", job.name)
                self.logger.error("-" * 80)
                self.logger.error("Exception: %s", type(job.ret).__name__)
                self.logger.error("Job ID: %s", job.id)
                self.logger.error("Host: %s", job.host_name)
                self.logger.error("." * 80)
                self.logger.error(job.traceback)
                raise job.ret

            # attempt to resubmit
            if job.cause_of_death:
                self.logger.info("Creating error report")

                # send report
                if SEND_ERROR_MAIL:
                    send_error_mail(job)

                # try to resubmit
                old_id = job.id
                job.track_cpu = []
                job.track_mem = []
                handle_resubmit(self.session_id, job, temp_dir=self.temp_dir)
                # Update job ID if successfully resubmitted
                self.logger.info('Resubmitted job %s; it now has ID %s',
                                 old_id,
                                 job.id)
                del self.id_to_job[old_id]
                self.id_to_job[job.id] = job

                # break out of loop to avoid too long delay
                break

    def all_jobs_done(self):
        """
        checks for all jobs if they are done
        """
        if self.logger.getEffectiveLevel() == logging.DEBUG:
            num_jobs = len(self.jobs)
            num_completed = sum((job.ret != _JOB_NOT_FINISHED and
                                 not isinstance(job.ret, Exception))
                                for job in self.jobs)
            self.logger.debug('%i out of %i jobs completed', num_completed,
                              num_jobs)

        # exceptions will be handled in check_if_alive
        return all((job.ret != _JOB_NOT_FINISHED and not isinstance(job.ret,
                                                                    Exception))
                   for job in self.jobs)


def send_error_mail(job):
    """
    send out diagnostic email
    """
    logger = logging.getLogger(__name__)

    # Connect to server
    try:
        s = smtplib.SMTP(SMTP_SERVER)
    except smtplib.SMTPConnectError:
        logger.error('Failed to connect to SMTP server to send error ' +
                     'email.', exc_info=True)
        return

    # create message
    msg = MIMEMultipart()
    msg["subject"] = "GridMap error {}".format(job.name)
    msg["From"] = ERROR_MAIL_SENDER
    msg["To"] = ERROR_MAIL_RECIPIENT

    # compose error message
    body_text = ""
    body_text += "Job {}\n".format(job.name)
    body_text += "Last timestamp: {}\n".format(job.timestamp)
    body_text += "Resubmissions: {}\n".format(job.num_resubmits)
    body_text += "Cause of death: {}\n".format(job.cause_of_death)

    if job.heart_beat:
        body_text += "Last memory usage: {}\n".format(job.heart_beat["memory"])
        body_text += "Last cpu load: {}\n".format(job.heart_beat["cpu_load"][0])
        body_text += ("Process was running at last check: " +
                      "{}\n\n").format(job.heart_beat["cpu_load"][1])

    body_text += "Host: {}\n\n".format(job.host_name)

    if isinstance(job.ret, Exception):
        body_text += "Job encountered exception: {}\n".format(job.ret)
        body_text += "Stacktrace: {}\n\n".format(job.traceback)

    logger.info('Email body: %s', body_text)

    body_msg = MIMEText(body_text)
    msg.attach(body_msg)

    # attach log file
    if job.heart_beat and "log_file" in job.heart_beat:
        log_file_attachement = MIMEText(job.heart_beat['log_file'])
        log_file_attachement.add_header('Content-Disposition', 'attachment',
                                        filename='{}_log.txt'.format(job.id))
        msg.attach(log_file_attachement)

    # if matplotlib is installed
    if CREATE_PLOTS:
        #TODO: plot to cstring directly (some code is there)
        #imgData = cStringIO.StringIO()
        #plt.savefig(imgData, format='png')

        # rewind the data
        #imgData.seek(0)
        #plt.savefig(imgData, format="png")

        time = [HEARTBEAT_FREQUENCY * i for i in range(len(job.track_mem))]

        # attack mem plot
        img_mem_fn = os.path.join('/tmp', "{}_mem.png".format(job.id))
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
        img_cpu_fn = os.path.join("/tmp", "{}_cpu.png".format(job.id))
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

    # Send mail
    try:
        s.sendmail(ERROR_MAIL_SENDER, ERROR_MAIL_RECIPIENT, msg.as_string())
    except (SMTPRecipientsRefused, SMTPHeloError, SMTPSenderRefused,
            SMTPDataError):
        logger.error('Failed to send error email.', exc_info=True)

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
    job.id set to new ID
    """
    # reset some fields
    job.timestamp = None
    job.heart_beat = None

    if job.num_resubmits < NUM_RESUBMITS:
        logger = logging.getLogger(__name__)
        logger.warning("Looks like job %s (%s) died an unnatural death, " +
                       "resubmitting (previous resubmits = %i)", job.name,
                       job.id, job.num_resubmits)

        # remove node from white_list
        node_name = '{}@{}'.format(job.queue, job.host_name)
        if job.white_list and node_name in job.white_list:
            job.white_list.remove(node_name)

        # increment number of resubmits
        job.num_resubmits += 1
        job.cause_of_death = ""

        _resubmit(session_id, job, temp_dir)
    else:
        raise JobException(("Job {0} ({1}) failed after {2} " +
                            "resubmissions").format(job.name, job.id,
                                                    NUM_RESUBMITS))


def _execute(job):
    """
    Cannot pickle method instances, so fake a function.
    Used by _process_jobs_locally
    """
    job.execute()
    return job.ret


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
        for ret_val, job in zip(result, jobs):
            job.ret = ret_val
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

    :returns: Session ID
    """
    with Session() as session:
        for job in jobs:
            # set job white list
            job.white_list = white_list

            # remember address of submission host
            job.home_address = home_address

            # append jobs
            _append_job_to_session(session, job, temp_dir=temp_dir, quiet=quiet)

        sid = session.contact
    return sid


def _append_job_to_session(session, job, temp_dir='/scratch/', quiet=True):
    """
    For an active session, append new job based on information stored in job
    object. Also sets job.id to the ID of the job on the grid.

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
    logger = logging.getLogger(__name__)
    # logger.debug('{}'.format(job.environment))
    jt.jobEnvironment = job.environment

    # Run module using python -m to avoid ImportErrors when unpickling jobs
    jt.remoteCommand = sys.executable
    jt.args = ['-m', 'gridmap.runner', '{}'.format(job.home_address), job.path]
    jt.nativeSpecification = job.native_specification
    jt.jobName = job.name
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

    job_id = session.runJob(jt)

    # set job fields that depend on the job_id assigned by grid engine
    job.id = job_id
    job.log_stdout_fn = os.path.join(temp_dir, '{}.o{}'.format(job.name,
                                                               job_id))
    job.log_stderr_fn = os.path.join(temp_dir, '{}.e{}'.format(job.name,
                                                               job_id))

    if not quiet:
        print('Your job {} has been submitted with id {}'.format(job.name,
                                                                 job_id),
              file=sys.stderr)

    session.deleteJobTemplate(jt)


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
        local = True

    if not local:
        # initialize monitor to get port number
        with JobMonitor(temp_dir=temp_dir) as monitor:
            # get interface and port
            home_address = monitor.home_address

            # job_id field is attached to each job object
            sid = _submit_jobs(jobs, home_address, temp_dir=temp_dir,
                               white_list=white_list, quiet=quiet)

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
                session.control(job.id, JobControlAction.TERMINATE)
                logger.info("zombie job killed")
            except Exception:
                logger.error("Could not kill job with SGE id %s", job.id,
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
             queue=DEFAULT_QUEUE, quiet=True, local=False, max_processes=1):
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
    :param local: Should we execute the jobs locally in separate processes
                  instead of on the the cluster?
    :type local: bool
    :param max_processes: The maximum number of concurrent processes to use if
                          processing jobs locally.
    :type max_processes: int

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
                               quiet=quiet, local=local,
                               max_processes=max_processes)

    return job_results

