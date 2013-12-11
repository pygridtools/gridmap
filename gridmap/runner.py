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


def _heart_beat(runner, address, wait_sec=45):
    """
    Infinitely loops and sends information about the currently running job back
    to the ``JobMonitor``.

    :param runner: The ``Runner`` we're monitoring. If ``None``, this is assumed
                   to be monitoring the ``JobMonitor`` itself.
    :type runner: ``Runner`` or ``NoneType``
    :param address: URL of ``JobMonitor``
    :type address: str
    :param wait_sec: The amount of time to wait between heartbeats.
    :type wait_sec: int
    """

    while True:
        if runner is not None:
            job_id = runner.job_id
            status = runner.status
            if os.path.exists(runner.log_file):
                with open(runner.log_file) as f:
                    status["log_file"] = f.read()
        else:
            job_id = -1
            status = {}
        _send_zmq_msg(job_id, "heart_beat", status, address)
        time.sleep(wait_sec)


class Runner(object):
    """
    Encapsulates a running ``Job`` that was retrieved from the
    ``JobMonitor``.
    """
    def __init__(self, job_id, monitor_url):
        super(Runner, self).__init__()
        self.job_id = job_id
        self.monitor_url = monitor_url
        self.process = psutil.Process(os.getpid())
        self.logger = logging.getLogger(__name__)
        self.job = None
        self.log_file = ''


    @property
    def total_memory(self):
        """
        Total memory usage of process (and children) in Mb.
        """
        mem_total = float(self.process.get_memory_info()[0])
        mem_total += sum(float(p.get_memory_info()[0]) for p in
                         self.process.get_children(recursive=True))
        return mem_total / (1024.0 ** 2.0)

    @property
    def cpu_info(self):
        """
        Tuple of average ratio of CPU time to real time for process and its
        children (excluding heartbeat process), and whether at least one of the
        processes is not sleeping.
        """
        cpu_sum = float(self.process.get_cpu_percent())
        running = self.process.status in _SLEEP_STATUSES
        num_procs = 1
        for p in self.process.get_children(recursive=True):
            cpu_sum += p.get_cpu_percent()
            running = running or p.status in _SLEEP_STATUSES
        return cpu_sum / num_procs, running

    @property
    def status(self):
        '''
        Determines the status of the current worker and its machine.

        :returns: Memory and CPU load information.
        :rtype: dict
        '''
        return {'memory': self.total_memory, 'cpu_load': self.cpu_info}

    def run(self):
        """
        Execute the pickled job and produce pickled output.
        """
        wait_sec = random.randint(0, 5)
        self.logger.info("Waiting %i seconds before starting", wait_sec)
        time.sleep(wait_sec)

        try:
            self.job = _send_zmq_msg(self.job_id, "fetch_input", None,
                                     self.monitor_url)
        except Exception as e:
            # here we will catch errors caused by pickled objects
            # of classes defined in modules not in PYTHONPATH
            self.logger.error('Could not retrieve input for job %s',
                              self.job_id, exc_info=True)

            # send back exception and traceback string
            thank_you_note = _send_zmq_msg(self.job_id, "store_output",
                                           (e, traceback.format_exc()),
                                           self.monitor_url)
            return

        self.logger.debug("input arguments loaded, starting computation %s",
                          self.job)

        # create heart beat process
        heart = multiprocessing.Process(target=_heart_beat,
                                        args=(self.self.job_id,
                                              self.monitor_url, self.pid,
                                              self.job.log_stderr_fn,
                                              HEARTBEAT_FREQUENCY))
        self.logger.info("Starting heart beat")
        heart.start()

        try:
            # change working directory
            if self.job.working_dir is not None:
                self.logger.info("Changing working directory: %s",
                                 self.job.working_dir)
                os.chdir(self.job.working_dir)

            # run job
            self.logger.info("Executing job")
            self.job.execute()
            self.logger.info('Finished job')

            # send back result
            thank_you_note = _send_zmq_msg(self.job_id, "store_output",
                                           self.job, self.monitor_url)
            self.logger.info(thank_you_note)

        finally:
            # stop heartbeat
            heart.terminate()


def _main():
    """
    Parse the command line inputs and call Runner.run
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

    # Load up the job and get things running
    runner = Runner(os.environ['JOB_ID'], args.home_address)
    runner.run()


if __name__ == "__main__":
    _main()
