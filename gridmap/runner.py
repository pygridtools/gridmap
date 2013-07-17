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
This module executes pickled jobs on the cluster.

@author: Christian Widmer
@author: Cheng Soon Ong
@author: Dan Blanchard (dblanchard@ets.org)
"""

from __future__ import absolute_import, print_function, unicode_literals

import argparse
import os
import sys

from redis import StrictRedis

from gridmap import REDIS_DB, REDIS_PORT
from gridmap.data import clean_path, zload_db, zsave_db


def _run_job(uniq_id, job_num, temp_dir, redis_host):
    """
    Execute the pickled job and produce pickled output.

    @param uniq_id: The unique suffix for the tables corresponding to this job
                    in the database.
    @type uniq_id: C{basestring}
    @param job_num: The index for this job's content in the job and output
                    tables.
    @type job_num: C{int}
    @param temp_dir: Local temporary directory for storing output for an
                     individual job.
    @type temp_dir: C{basestring}
    @param redis_host: Hostname of the database to connect to get the job data.
    @type redis_host: C{basestring}
    """
    # Connect to database
    redis_server = StrictRedis(host=redis_host, port=REDIS_PORT, db=REDIS_DB)

    print("Loading job...", end="", file=sys.stderr)
    sys.stderr.flush()
    try:
        import pdb
        pdb.set_trace()
        job = zload_db(redis_server, 'job{0}'.format(uniq_id), job_num)
    except Exception as detail:
        job = None
        print("FAILED", file=sys.stderr)

        print("Writing exception to database for job {0}...".format(job_num),
              end="", file=sys.stderr)
        sys.stderr.flush()
        zsave_db(detail, redis_server, 'output{0}'.format(uniq_id), job_num)
        print("done", file=sys.stderr)
    else:
        print("done", file=sys.stderr)

        print("Running job...", end="", file=sys.stderr)
        sys.stderr.flush()
        job.execute()
        print("done", file=sys.stderr)

        print("Writing output to database for job {0}...".format(job_num),
              end="", file=sys.stderr)
        sys.stderr.flush()
        zsave_db(job.ret, redis_server, 'output{0}'.format(uniq_id), job_num)
        print("done", file=sys.stderr)

        #remove files
        if job.cleanup:
            log_stdout_fn = os.path.join(temp_dir, '{0}.o{1}'.format(job.name,
                                                                     job.jobid))
            log_stderr_fn = os.path.join(temp_dir, '{0}.e{1}'.format(job.name,
                                                                     job.jobid))

            try:
                os.remove(log_stdout_fn)
                os.remove(log_stderr_fn)
            except OSError:
                pass


def _main():
    """
    Parse the command line inputs and call _run_job
    """

    # Get command line arguments
    parser = argparse.ArgumentParser(description="This wrapper script will run \
                                                 a pickled Python function on \
                                                 some pickled data in a Redis\
                                                 database, " + "and write the\
                                                 results back to the database.\
                                                 You almost never want to run\
                                                 this yourself.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     conflict_handler='resolve')
    parser.add_argument('uniq_id',
                        help='The unique suffix for the tables corresponding to\
                              this job in the database.')
    parser.add_argument('job_number',
                        help='Which job number should be run. Dictates which \
                              input data is read from database and where output\
                              data is stored.',
                        type=int)
    parser.add_argument('module_dir',
                        help='Directory that contains module containing pickled\
                              function. This will get added to PYTHONPATH \
                              temporarily.')
    parser.add_argument('temp_dir',
                        help='Directory that temporary output will be stored\
                              in.')
    parser.add_argument('redis_host',
                        help='The hostname of the server that where the Redis\
                              database is.')
    args = parser.parse_args()

    print("Appended {0} to PYTHONPATH".format(args.module_dir), file=sys.stderr)
    sys.path.append(clean_path(args.module_dir))

    # Process the database and get job started
    _run_job(args.uniq_id, args.job_number, clean_path(args.temp_dir),
             args.redis_host)


if __name__ == "__main__":
    _main()
