#!/usr/bin/env python

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
In addition to the high level map interface "grid_map", gridmap also allows one
to easily create a list of jobs (that potentially run different functions) and
execute them on the cluster as well.
"""

from __future__ import print_function, unicode_literals

import time

from gridmap import Job, process_jobs


def compute_factorial(n):
    """
    computes factorial of n
    """
    time.sleep(10)
    ret = 1
    for i in range(n):
        ret = ret * (i + 1)
    return ret


def make_jobs():
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
        job = Job(compute_factorial, arg)
        jobs.append(job)

    return jobs


def main():
    """
    run a set of jobs on cluster
    """

    print("=====================================")
    print("========   Submit and Wait   ========")
    print("=====================================")
    print("")

    functionJobs = make_jobs()

    print("sending function jobs to cluster")
    print("")

    job_outputs = process_jobs(functionJobs)

    print("results from each job")
    for (i, result) in enumerate(job_outputs):
        print("Job {0}- result: {1}".format(i, result))


if __name__ == "__main__":
    main()

