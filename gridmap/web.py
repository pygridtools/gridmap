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
Simple web front-end for pythongrid

:author: Christian Widmer
:author: Cheng Soon Ong
:author: Dan Blanchard (dblanchard@ets.org)
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse
import logging
from io import open
from socket import gethostname

import cherrypy

from gridmap.conf import WEB_PORT
from gridmap.runner import _send_zmq_msg


class WebMonitor(object):
    @cherrypy.expose
    def index(self):
        return '''
               <form action="list_jobs" method="GET">
               Address of pythongrid session: <br />
               <input type="text" name="address" /><br /><br />
               <input type="submit" />
               </form>
               '''

    @cherrypy.expose
    def list_jobs(self, address):
        """
        display list of jobs
        """
        job_id = -1
        jobs = _send_zmq_msg(job_id, "get_jobs", "", address)

        out_html = '''
            <form action="list_jobs" method="GET">
            Address of pythongrid session: <br />
            <input type="text" name="address" /><br /><br />
            <input type="submit" /><br><br><br>
            </form>

            <form action="view_job" method="GET">
            <table border="1">
            <tr><td>sge job id</td><td>job done</td><td>cause of death</td><tr>
            '''

        for job in jobs:
            out_html += ("<tr><td><a href='/view_job?address={}" +
                         "&job_id={}'>{}</td>").format(address, job.name,
                                                       job.jobid)
            out_html += "<td>{}</td>".format(job.ret is not None)
            out_html += "<td>{}</td>".format(job.cause_of_death)
            out_html += "</tr>"
        out_html += "</table></form>"
        return out_html

    @cherrypy.expose
    def view_job(self, address, job_id):
        """
        display individual job details
        """
        job = _send_zmq_msg(job_id, "get_job", "", address)
        out_html = ""
        details = self.job_to_html(job)
        out_html += details
        return out_html

    @staticmethod
    def job_to_html(job):
        """
        display job as html
        """

        # compose error message
        body_text = "job {0}\n<br>".format(job.name)
        body_text += "last timestamp: {0}\n<br>".format(job.timestamp)
        body_text += "num_resubmits: {0}\n<br>".format(job.num_resubmits)
        body_text += "cause_of_death: {0}\n<br>".format(job.cause_of_death)

        if job.heart_beat:
            body_text += ("last memory usage: {0}\n" +
                          "<br>").format(job.heart_beat["memory"])
            body_text += ("last cpu load: {0}\n" +
                          "<br>").format(job.heart_beat["cpu_load"])

        body_text += "host: {}<br><br>\n\n".format(job.host_name)

        if isinstance(job.ret, Exception):
            body_text += "job encountered exception: {0}\n<br>".format(job.ret)
            body_text += "stacktrace: {0}\n<br>\n<br>".format(job.exception)

        # attach log file
        if job.heart_beat:
            with open(job.heart_beat["log_file"], "r") as log_file:
                log_file_attachement = log_file.read().replace("\n", "<br>\n")
            body_text += "<br><br><br>" + log_file_attachement
        return body_text


def _main():
    """
    Parse the command line inputs and start web monitor.
    """
    # Get command line arguments
    parser = argparse.ArgumentParser(description="Provides a web interface to \
                                                  0MQ job monitor.")
    parser.parse_args()

    # Make warnings from built-in warnings module get formatted more nicely
    logging.captureWarnings(True)
    logging.basicConfig(format=('%(asctime)s - %(name)s - %(levelname)s - ' +
                                '%(message)s'))

    # Start server
    hostname = gethostname()
    if isinstance(hostname, bytes):
        hostname = hostname.decode()
    cherrypy.quickstart(WebMonitor(),
                        config={'global': {'server.socket_port': WEB_PORT,
                                           'server.socket_host': hostname}})


if __name__ == "__main__":
    _main()
