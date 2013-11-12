# -*- coding: utf-8 -*-

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2013 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2013 ETS

# This file is part of Grid Map.

# Grid Map is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Grid Map is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Grid Map.  If not, see <http://www.gnu.org/licenses/>.
"""
JobMonitor and its related helper functions.

:author: Christian Widmer
:author: Cheng Soon Ong
:author: Dan Blanchard (dblanchard@ets.org)
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import logging
import multiprocessing
import socket
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from io import open
from subprocess import Popen

import zmq

from gridmap.conf import (CHECK_FREQUENCY, CREATE_PLOTS, USE_CHERRYPY,
                          MAX_TIME_BETWEEN_HEARTBEATS, MAX_MSG_LENGTH,
                          SMTP_SERVER, SEND_ERROR_MAILS, ERROR_MAIL_RECIPIENT,
                          ERROR_MAIL_SENDER, NUM_RESUBMITS)
from gridmap.data import zloads, zdumps
from gridmap.job import _resubmit
from gridmap.runner import _heart_beat


class JobMonitor(object):
    """
    Job monitor that communicates with other nodes via 0MQ.
    """

    def __init__(self):
        """
        set up socket
        """
        logger = logging.getLogger(__name__)

        context = zmq.Context()
        self.socket = context.socket(zmq.REP)

        self.host_name = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.host_name)
        self.interface = "tcp://%s" % (self.ip_address)

        # bind to random port and remember it
        self.port = self.socket.bind_to_random_port(self.interface)
        self.home_address = "%s:%i" % (self.interface, self.port)

        logger.info("setting up connection on %s", self.home_address)

        if USE_CHERRYPY:
            logger.info("starting web interface")
            # TODO: Check that it isn't already running
            Popen([sys.executable, "-m", "gridmap.web"])

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
        self.jobid_to_job = {job.name: job for job in jobs}

        # determines in which interval to check if jobs are alive
        local_heart = multiprocessing.Process(target=_heart_beat,
                                              args=(-1, self.home_address, -1,
                                                    "", CHECK_FREQUENCY))
        local_heart.start()
        logger.info("Using ZMQ layer to keep track of jobs")
        # main loop
        while not self.all_jobs_done():

            msg_str = self.socket.recv()
            msg = zloads(msg_str)

            return_msg = ""

            job_id = msg["job_id"]

            # only if its not the local beat
            if job_id != -1:

                job = self.jobid_to_job[job_id]
                logger.info('Received message: %s', msg)

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
                    job.log_file = msg["data"]["log_file"]

                    # keep track of mem and cpu
                    try:
                        job.track_mem.append(float(job.heart_beat["memory"]))
                        job.track_cpu.append(float(job.heart_beat["cpu_load"]))
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
            self.socket.send(zdumps(return_msg))

        local_heart.terminate()


    def check_if_alive(self):
        """
        check if jobs are alive and determine cause of death if not
        """
        logger = logging.getLogger(__name__)
        for job in self.jobs:

            # noting was returned yet
            if job.ret == None:

                # exclude first-timers
                if job.timestamp != None:

                    # only check heart-beats if there was a long delay
                    current_time = datetime.now()
                    time_delta = current_time - job.timestamp

                    if time_delta.seconds > MAX_TIME_BETWEEN_HEARTBEATS:
                        logger.error("job died for unknown reason")
                        job.cause_of_death = "unknown"
            else:

                # could have been an exception, we check right away
                if job.is_out_of_memory():
                    logger.error("job was out of memory")
                    job.cause_of_death = "out_of_memory"
                    job.ret = None

                elif isinstance(job.ret, Exception):
                    logger.error("job encountered exception, will not resubmit")
                    job.cause_of_death = "exception"
                    send_error_mail(job)
                    job.ret = "job dead (with non-memory related exception)"


            # attempt to resubmit
            if job.cause_of_death in frozenset("out_of_memory", "unknown"):
                logger.info("creating error report")

                # send report
                send_error_mail(job)

                # try to resubmit
                if not handle_resubmit(self.session_id, job):
                    logger.error("giving up on job")
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
        body_text += "last cpu load: {}\n\n".format(job.heart_beat["cpu_load"])

    body_text += "host: {}\n\n".format(job.host_name)

    if isinstance(job.ret, Exception):
        body_text += "job encountered exception: {}\n".format(job.ret)
        body_text += "stacktrace: {}\n\n".format(job.exception)

    logger.info('Email body: %s', body_text)

    body_msg = MIMEText(body_text)
    msg.attach(body_msg)

    # attach log file
    if job.heart_beat:
        with open(job.heart_beat["log_file"], "r") as log_file:
            log_file_attachement = MIMEText(log_file.read())
        msg.attach(log_file_attachement)

    # if matplotlib is installed
    if CREATE_PLOTS:
        import matplotlib.pyplot as plt
        #TODO: plot to cstring directly (some code is there)
        #imgData = cStringIO.StringIO()
        #plt.savefig(imgData, format='png')

        # rewind the data
        #imgData.seek(0)
        #plt.savefig(imgData, format="png")

        # attack mem plot
        img_mem_fn = "/tmp/" + job.name + "_mem.png"
        plt.figure()
        plt.plot(job.track_mem, "-o")
        plt.title("memory usage")
        plt.savefig(img_mem_fn)
        with open(img_mem_fn, "rb") as img_mem:
            img_mem_attachement = MIMEImage(img_mem.read())
        msg.attach(img_mem_attachement)

        # attach cpu plot
        img_cpu_fn = "/tmp/" + job.name + "_cpu.png"
        plt.figure()
        plt.plot(job.track_cpu, "-o")
        plt.title("cpu load")
        plt.savefig(img_cpu_fn)
        with open(img_cpu_fn, "rb") as img_cpu:
            img_cpu_attachement = MIMEImage(img_cpu.read())
        msg.attach(img_cpu_attachement)

    if SEND_ERROR_MAILS:
        import smtplib
        s = smtplib.SMTP(SMTP_SERVER)
        s.sendmail(ERROR_MAIL_SENDER, ERROR_MAIL_RECIPIENT,
                   msg.as_string()[0:MAX_MSG_LENGTH])
        s.quit()


def handle_resubmit(session_id, job):
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
        if job.white_list.count(node_name) > 0:
            job.white_list.remove(node_name)

        # increment number of resubmits
        job.num_resubmits += 1
        job.cause_of_death = ""

        _resubmit(session_id, job)

        return True
    else:
        return False

