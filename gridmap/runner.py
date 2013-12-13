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
This module executes pickled jobs on the cluster.

:author: Christian Widmer
:author: Cheng Soon Ong
:author: Dan Blanchard (dblanchard@ets.org)
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse
import logging
import multiprocessing
import os
import random
import socket
import sys
import time
import traceback
from io import open

import psutil
import zmq

# Import QueueHandler and QueueListener for multiprocess-safe logging
if sys.version_info < (3, 0):
    from logutils.queue import QueueHandler, QueueListener
else:
    from logging.handlers import QueueHandler, QueueListener

from gridmap.conf import HEARTBEAT_FREQUENCY
from gridmap.data import clean_path, zloads, zdumps


# Set of "not running" job statuses
_SLEEP_STATUSES = {psutil.STATUS_SLEEPING, psutil.STATUS_DEAD,
                   psutil.STATUS_IDLE, psutil.STATUS_STOPPED,
                   psutil.STATUS_ZOMBIE}


# TODO: Refactor this so that there's a class that stores socket, since creating
#       a new one each time is hugely wasteful.
def _send_zmq_msg(job_id, command, data, address):
    """
    simple code to send messages back to host
    (and get a reply back)
    """
    logger = logging.getLogger(__name__)
    context = zmq.Context()
    zsocket = context.socket(zmq.REQ)
    logger.debug('Connecting to JobMonitor (%s)', address)
    zsocket.connect(address)

    host_name = socket.gethostname()
    ip_address = socket.gethostbyname(host_name)

    msg_container = {}
    msg_container["job_id"] = job_id
    msg_container["host_name"] = host_name
    msg_container["ip_address"] = ip_address
    msg_container["command"] = command
    msg_container["data"] = data

    # Send request
    logger.debug('Sending message: %s', msg_container)
    msg_string = zdumps(msg_container)
    zsocket.send(msg_string)

    # Get reply
    msg = zloads(zsocket.recv())

    return msg


def _heart_beat(job_id, address, parent_pid=-1, log_file="", wait_sec=45,
                queue=None, log_level=logging.DEBUG):
    """
    Infinitely loops and sends information about the currently running job back
    to the ``JobMonitor``.
    """
    # Setup logging
    if queue is not None:
        handler = QueueHandler(queue)
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        logger.setLevel(log_level)
    while True:
        status = get_job_status(parent_pid, os.getpid())
        if os.path.exists(log_file):
            with open(log_file) as f:
                status["log_file"] = f.read()
        _send_zmq_msg(job_id, "heart_beat", status, address)
        time.sleep(wait_sec)


def get_memory_usage(pid, heart_pid):
    """
    :param pid: Process ID for job whose memory usage we'd like to check.
    :type pid: int
    :param heart_pid: ID of the heartbeat process, which will not be counted
                      toward total.
    :type heart_pid: int

    :returns: Total memory usage of process (and children) in Mb.
    """
    process = psutil.Process(pid)
    mem_total = float(process.get_memory_info()[0])
    mem_total += sum(float(p.get_memory_info()[0]) for p in
                     process.get_children(recursive=True)
                     if process.pid != heart_pid)
    return mem_total / (1024.0 ** 2.0)


def get_cpu_load(pid, heart_pid):
    """
    :param pid: Process ID for job whose CPU load we'd like to check.
    :type pid: int
    :param heart_pid: ID of the heartbeat process, which will not be counted
                      toward total.
    :type heart_pid: int

    :returns: Tuple of average ratio of CPU time to real time for process and
              its children (excluding heartbeat process), and whether at least
              one of the processes is not sleeping.
    :rtype: (float, bool)
    """
    logger = logging.getLogger(__name__)
    logger.debug('Checking load for PID %s with heart PID %s', pid, heart_pid)
    process = psutil.Process(pid)
    cpu_sum = float(process.get_cpu_percent())
    logger.debug('Parent process percentage: %s', cpu_sum)
    logger.debug('Parent running status: %s', process.status)
    running = process.status not in _SLEEP_STATUSES
    num_procs = 1
    for p in process.get_children(recursive=True):
        logger.debug('Child PID %s', p.pid)
        if p.pid != heart_pid:
            logger.debug('Child process percentage: %s', p.get_cpu_percent())
            logger.debug('Child running status: %s', p.status)
            cpu_sum += float(p.get_cpu_percent())
            running = running or (p.status not in _SLEEP_STATUSES)
            num_procs += 1
    return cpu_sum / num_procs, running


def get_job_status(parent_pid, heart_pid):
    """
    Determines the status of the current worker and its machine

    :param parent_pid: Process ID for job whose status we'd like to check.
    :type parent_pid: int
    :param heart_pid: ID of the heartbeat process, which will not be counted
                      toward total.
    :type heart_pid: int

    :returns: Memory and CPU load information for given PID.
    :rtype: dict
    """

    status_container = {}

    if parent_pid != -1:
        status_container["memory"] = get_memory_usage(parent_pid, heart_pid)
        status_container["cpu_load"] = get_cpu_load(parent_pid, heart_pid)

    return status_container


def _run_job(job_id, address):
    """
    Execute the pickled job and produce pickled output.

    :param job_id: Unique ID of job
    :type job_id: int
    :param address: IP address of submitting host.
    :type address: str
    """
    wait_sec = random.randint(0, 5)
    logger = logging.getLogger(__name__)
    logger.info("Waiting %i seconds before starting", wait_sec)
    time.sleep(wait_sec)

    try:
        job = _send_zmq_msg(job_id, "fetch_input", None, address)
    except Exception as e:
        # here we will catch errors caused by pickled objects
        # of classes defined in modules not in PYTHONPATH
        logger.error('Could not retrieve input for job {0}'.format(job_id),
                     exc_info=True)

        # send back exception and traceback string
        thank_you_note = _send_zmq_msg(job_id, "store_output",
                                       (e, traceback.format_exc()),
                                       address)
        return

    logger.debug("Input arguments loaded, starting computation %s", job)

    # Create thread/process-safe logger stuff
    queue = multiprocessing.Queue(-1)
    q_handler = QueueHandler(queue)
    logger.addHandler(q_handler)
    q_listener = QueueListener(queue)
    q_listener.start()

    # create heart beat process
    parent_pid = os.getpid()
    heart = multiprocessing.Process(target=_heart_beat,
                                    args=(job_id, address, parent_pid,
                                          job.log_stderr_fn,
                                          HEARTBEAT_FREQUENCY, queue,
                                          logger.getEffectiveLevel()))
    logger.info("Starting heart beat")
    heart.start()

    try:
        # change working directory
        if job.working_dir is not None:
            logger.info("Changing working directory: %s", job.working_dir)
            os.chdir(job.working_dir)

        # run job
        logger.info("Executing job")
        job.execute()
        logger.info("Finished job")

        # send back result
        thank_you_note = _send_zmq_msg(job_id, "store_output", job, address)
        logger.info(thank_you_note)

    finally:
        # stop heartbeat
        heart.terminate()

        # Tear-down thread/process-safe logging and switch back to regular
        q_listener.stop()
        logger.removeHandler(q_handler)


def _main():
    """
    Parse the command line inputs and call _run_job
    """

    # Get command line arguments
    parser = argparse.ArgumentParser(description="This wrapper script will run \
                                                  a pickled Python function on \
                                                  some pickled retrieved data \
                                                  via 0MQ. You almost never \
                                                  want to run this yourself.")
    parser.add_argument('home_address',
                        help='IP address of submitting host.')
    parser.add_argument('module_dir',
                        help='Directory that contains module containing pickled\
                              function. This will get added to PYTHONPATH \
                              temporarily.')
    args = parser.parse_args()

    # Make warnings from built-in warnings module get formatted more nicely
    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +
                                '%(message)s'), level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Appended {0} to PYTHONPATH".format(args.module_dir))
    sys.path.append(clean_path(args.module_dir))

    logger.debug("Job ID: %i\tHome address: %s\tModule dir: %s",
                 os.environ['JOB_ID'],
                 args.home_address, args.module_dir)

    # Process the database and get job started
    _run_job(os.environ['JOB_ID'], args.home_address)


if __name__ == "__main__":
    _main()
