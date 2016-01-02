#!/usr/bin/env python

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
In addition to the high level map interface "grid_map", gridmap also allows one
to easily create a list of jobs (that potentially run different functions) and
execute them on the cluster as well.
"""

from __future__ import print_function, unicode_literals

import logging
from datetime import datetime

from gridmap import Job, process_jobs
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--engine', help='Name of the grid engine you are using.', choices=['TOURQUE','PBS','SGE'], default='SGE')
parser.add_argument('--queue', help='Name of the queue you want to send jobs to.', default='all.q')
parser.add_argument('--vmem', help='Amount of memory to use on a node.', default='200m')
parser.add_argument('--port', help='The port through which to communicate with the JobMonitor', default=None, type=int)
parser.add_argument('--local', help='Flag indicating whether the jobs should run locally instead of on the cluster', default=False, type=bool)
parser.add_argument("--logging", type=str, choices=['INFO', 'DEBUG', 'WARN'], help='increase output verbosity', default='INFO')
def sleep_walk(secs):
    '''
    Pass the time by adding numbers until the specified number of seconds has
    elapsed. Intended as a replacement for ``time.sleep`` that doesn't leave the
    CPU idle (which will make the job seem like it's stalled).
    '''
    start_time = datetime.now()
    num = 0
    while (datetime.now() - start_time).seconds < secs:
        num = num + 1


def compute_factorial(n):
    """
    computes factorial of n
    """
    sleep_walk(10)
    ret = 1
    for i in range(n):
        ret = ret * (i + 1)
    return ret


def make_jobs(engine, queue, vmem):
    """
    creates a list of Job objects,
    which carry all information needed
    for a function to be executed on SGE:
    - function object
    - arguments
    - settings
    """

    # set up list of arguments
    inputvec = [[3], [5], [10], [20]]

    # create empty job vector
    jobs = []

    # create job objects
    for arg in inputvec:
        # The default queue used by the Job class is all.q. You must specify
        # the `queue` keyword argument if that is not the name of your queue.
        job = Job(compute_factorial, arg, queue=queue, engine=engine,
                                    mem_free=vmem)
        jobs.append(job)

    return jobs


def main():
    """
    run a set of jobs on cluster
    """

    args = parser.parse_args()
    engine = args.engine
    queue = args.queue
    vmem = args.vmem
    port = args.port
    local =args.local
    level = args.logging

    if level is 'DEBUG':
        level = logging.DEBUG
    elif level is 'WARN':
        level = logging.WARN
    elif level is 'INFO':
        level = logging.INFO

    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +  '%(message)s'), level=level)

    print("=====================================")
    print("========   Submit and Wait   ========")
    print("=====================================\n")


    functionJobs = make_jobs(engine, queue, vmem)
    if local :
        print('Running jobs locally')
    else:
        print("Sending function jobs to cluster engine: {}. Into queue: {} \n".format(engine, queue))


    job_outputs = process_jobs(functionJobs, max_processes=4, port=port, local=local)

    print("results from each job")
    for (i, result) in enumerate(job_outputs):
        print("Job {0}- result: {1}".format(i, result))


if __name__ == "__main__":
    main()
