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
from io import open
from subprocess import check_output

import zmq

from gridmap.conf import HEARTBEAT_FREQUENCY
from gridmap.data import clean_path, zloads, zdumps


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


def _heart_beat(job_id, address, parent_pid=-1, log_file="", wait_sec=45):
    """
    will send reponses to the server with
    information about the current state of
    the process
    """

    while True:
        status = get_job_status(parent_pid)
        status["log_file"] = log_file
        _send_zmq_msg(job_id, "heart_beat", status, address)
        time.sleep(wait_sec)


def _VmB(VmKey, pid):
    """
    get various mem usage properties of process with id pid in MB
    """

    _proc_status = '/proc/%d/status' % pid

    _scale = {'kB': 1.0/1024.0, 'mB': 1.0,
              'KB': 1.0/1024.0, 'MB': 1.0}

     # get pseudo file  /proc/<pid>/status
    try:
        with open(_proc_status) as t:
            v = t.read()
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
    :param pid: Process ID for job whose memory usage we'd like to check.
    :type pid: int

    :returns: Memory usage of process in Mb.
    """

    return _VmB('VmSize:', pid)


def get_cpu_load(pid):
    """
    :param pid: Process ID for job whose CPU load we'd like to check.
    :type pid: int

    :returns: CPU usage of process as ratio of cpu time to real time, and
              process state.
    :rtype: (float, str)
    """


    command = ["ps", "h", "-o", "pcpu,state", "-p", "%d" % (pid)]

    try:
        cpu_load, state = check_output(command).strip().split()
        cpu_load = float(cpu_load)
    except:
        logger = logging.getLogger(__name__)
        logger.warning('Getting CPU info failed.', exc_info=True)
        cpu_load = float('NaN')
        state = '?'

    return cpu_load, state


def get_job_status(parent_pid):
    """
    Determines the status of the current worker and its machine (currently not
    cross-platform)

    :param parent_pid: Process ID for job whose status we'd like to check.
    :type parent_pid: int

    :returns: Memory and CPU load information for given PID.
    :rtype: dict
    """

    status_container = {}

    if parent_pid != -1:
        status_container["memory"] = get_memory_usage(parent_pid)
        status_container["cpu_load"] = get_cpu_load(parent_pid)

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
    logger.info("waiting %i seconds before starting", wait_sec)
    time.sleep(wait_sec)

    try:
        job = _send_zmq_msg(job_id, "fetch_input", None, address)
    except Exception as e:
        # here we will catch errors caused by pickled objects
        # of classes defined in modules not in PYTHONPATH
        logger.error('Could not retrieve input for job {0}'.format(job_id),
                     exc_info=True)

        # send back exception
        thank_you_note = _send_zmq_msg(job_id, "store_output", e, address)
        return

    logger.debug("input arguments loaded, starting computation %s", job)

    # create heart beat process
    parent_pid = os.getpid()
    heart = multiprocessing.Process(target=_heart_beat,
                                    args=(job_id, address, parent_pid,
                                          job.log_stderr_fn,
                                          HEARTBEAT_FREQUENCY))
    logger.info("starting heart beat")
    heart.start()

    # change working directory
    if job.working_dir is not None:
        logger.info("Changing working directory: %s", job.working_dir)
        os.chdir(job.working_dir)

    # run job
    logger.info("executing job")
    job.execute()

    # send back result
    thank_you_note = _send_zmq_msg(job_id, "store_output", job, address)
    logger.info(thank_you_note)

    # stop heartbeat
    heart.terminate()


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
