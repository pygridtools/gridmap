#! /usr/bin/env python
# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2011 Christian Widmer
# Copyright (C) 2011 Max-Planck-Society
"""

simple web front-end for pythongrid
@author Christian Widmer

"""

import cherrypy
import sys

from pythongrid import send_zmq_msg


class WelcomePage:

    @cherrypy.expose
    def index(self):

        return '''
            <form action="list_jobs" method="GET">
            Address of pythongrid session: <br />
            <input type="text" name="address" /><br /><br />
            <input type="submit" />
            </form>'''

    def list_jobs(self, address):
        """
        display list of jobs
        self.num_resubmits = 0
        self.cause_of_death = ""
        self.jobid = -1
        self.host_name = ""
        self.timestamp = None
        self.heart_beat = None
        self.exception = None
        """

        job_id = -1
        self.jobs = send_zmq_msg(job_id, "get_jobs", "", address)

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


        for job in self.jobs:
            out_html += "<tr><td><a href='/view_job?address=%s&job_id=%s'>%s</td>" % (address, str(job.name), str(job.jobid))
            out_html += "<td>%s</td>" % (str(job.ret!=None))
            out_html += "<td>%s</td>" % (job.cause_of_death)

            out_html += "</tr>"

        out_html += "</table></form>"

        return out_html
    list_jobs.exposed = True


    def view_job(self, address, job_id):
        """
        display list of jobs
        self.num_resubmits = 0
        self.cause_of_death = ""
        self.jobid = -1
        self.host_name = ""
        self.timestamp = None
        self.heart_beat = None
        self.exception = None
        """

        job = send_zmq_msg(job_id, "get_job", "", address)

        out_html = ""

        details = job_to_html(job)

        out_html += details


        return out_html
    view_job.exposed = True



def job_to_html(job):
    """
    display job as html
    """

    # compose error message
    body_text = ""

    body_text += "job " + str(job.name) + "\n<br>"
    body_text += "last timestamp: " + str(job.timestamp) + "\n<br>"
    body_text += "num_resubmits: " + str(job.num_resubmits) + "\n<br>"
    body_text += "cause_of_death: " + str(job.cause_of_death) + "\n<br>"

    if job.heart_beat:
        body_text += "last memory usage: " + str(job.heart_beat["memory"]) + "\n<br>"
        body_text += "last cpu load: " + str(job.heart_beat["cpu_load"]) + "\n<br>"

    body_text += "requested memory: " + str(job.h_vmem) + "\n<br>"
    body_text += "host: <a href='http://sambesi.kyb.local/ganglia/?c=Three Towers&h=%s&m=load_one&r=hour&s=descending&hc=5&mc=2'>%s</a><br><br>\n\n" % (job.host_name, job.host_name)

    if isinstance(job.ret, Exception):
        body_text += "job encountered exception: " + str(job.ret) + "\n<br>"
        body_text += "stacktrace: " + str(job.exception) + "\n<br>\n<br>"


    # attach log file
    if job.heart_beat:
        log_file = open(job.heart_beat["log_file"], "r")
        log_file_attachement = log_file.read().replace("\n", "<br>\n")
        log_file.close()

        body_text += "<br><br><br>" + log_file_attachement


    return body_text





cherrypy.tree.mount(WelcomePage())

if __name__ == '__main__':
    print sys.argv
    import os.path
    thisdir = os.path.dirname(__file__)
    response = cherrypy.response
    #response.headers['Content-Type'] = 'application/json'
    #response.body = encoder.iterencode(response.body)
    cherrypy.quickstart(config=os.path.join(thisdir, 'pythongrid_web.conf'))

