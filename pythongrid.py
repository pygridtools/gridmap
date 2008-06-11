#! /usr/bin/env python
"""
pythongrid provides a nice frontend to DRMAA
"""

#paths on cluster file system
PYTHONPATH = ["~/svn/tools/python/", "~/svn/tools/python/pythongrid/"]


#location of pythongrid.py on cluster file system
DRMAAPATH = "/usr/local/sge/lib/lx26-amd64/"
LOCATION = "~/svn/tools/python/pythongrid/pythongrid.py"

#define tmp directories
TMPHOST = "~/tmp/DRMAA_JOB_OUT/"
TMPCLIENT = "~/tmp/DRMAA_JOB_OUT/"



import sys
import os
import bz2
import cPickle
import getopt
import threading

os.putenv("PYTHONPATH", str(os.getenv("LD_LIBRARY_PATH")) + ":" \
          + "/fml/ag-raetsch/one/home/lib/python2.5/site-packages")

#try to import DRMAA

#os.putenv("LD_LIBRARY_PATH", str(os.getenv("LD_LIBRARY_PATH")) + ":" + os.path.expanduser(DRMAAPATH))

#os.environ['LD_LIBRARY_PATH'] = os.getenv("LD_LIBRARY_PATH") + ":" + DRMAAPATH
#os.environ['LD_LIBRARY_PATH'] = DRMAAPATH

#print "env:", os.getenv("LD_LIBRARY_PATH")


drmaa_present=1

try:
    import DRMAA
except ImportError, detail:
    print "Error importing DRMAA. Only local multi-threading supported. Please check your installation."
    print detail
    drmaa_present=0



def getStatus(joblist):
    """
    Get the status of all jobs in joblist.
    """
    _decodestatus = {
        DRMAA.Session.UNDETERMINED: 'process status cannot be determined',
        DRMAA.Session.QUEUED_ACTIVE: 'job is queued and active',
        DRMAA.Session.SYSTEM_ON_HOLD: 'job is queued and in system hold',
        DRMAA.Session.USER_ON_HOLD: 'job is queued and in user hold',
        DRMAA.Session.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
        DRMAA.Session.RUNNING: 'job is running',
        DRMAA.Session.SYSTEM_SUSPENDED: 'job is system suspended',
        DRMAA.Session.USER_SUSPENDED: 'job is user suspended',
        DRMAA.Session.DONE: 'job finished normally',
        DRMAA.Session.FAILED: 'job finished, but failed',
        }

    s = DRMAA.Session()
    s.init()
    status_summary = {}.fromkeys(_decodestatus,0)
    for jobid in joblist:
        status_summary[s.getJobProgramStatus(jobid)] += 1

    for curkey in status_summary.keys():
        if status_summary[curkey]>0:
            print '%s: %d' % (_decodestatus[status_summary[curkey]],status_summary[curkey])






class Job (object):
    """
    Central entity that wrappes a function and its data. Basically,
    a job consists of a function, its argument list, its
    keyword list and a field "ret" which is filled, when
    the execute method gets called
    """

    f=None
    args=()
    kwlist={}
    ret=None
    cleanup=True

    nativeSpecification=""


    def __init__(self, f, args, kwlist={}, additionalpaths=[], cleanup=True):
        """
        constructor of Job

        @param f: a function, which should be executed.
        @type f: function
        @param args: argument list of function f
        @type args: list
        @param kwlist: dictionary of keyword arguments
        @type kwlist: dict
        @param additionalpaths: additional paths for use on cluster nodes
        @type additionalpaths: list of strings
        """

        self.f=f
        self.args=args
        self.kwlist=kwlist

        #fetch current pythonpath
        self.pythonpath=sys.path
        self.pythonpath.append(os.getcwd())

        self.cleanup=cleanup

        #TODO: set additional path from a config file
        #self.pythonpath.extend(additionalpaths)


    def __repr__(self):
        return ('%s\nargs=%s\nkwlist=%s\nret=%s\ncleanup=%s\nnativeSpecification=%s\n' %\
                (self.f,self.args,self.kwlist,self.ret,self.cleanup,self.nativeSpecification))

    def execute(self):
        """
        Executes function f with given arguments and writes return value to field ret.
        Input data is removed after execution to save space.
        """
        self.ret=apply(self.f, self.args, self.kwlist)



class KybJob(Job):
    """
    Specialization of generic Job that provides an interface to Kyb-specific options.
    Not quite finished yet, interface will most likely change in the near future. (march 2008)
    """

    name=""
    h_vmem=""
    arch=""
    tmpfree=""
    h_cpu=""
    h_rt=""
    express=""
    matlab=""
    simulink=""
    compiler=""
    imagetb=""
    opttb=""
    stattb=""
    sigtb=""
    cplex=""
    nicetohave=""

    #http://www.python.org/download/releases/2.2.3/descrintro/#property

    def getNativeSpecification(self):
        """
        define python-style getter
        """

        ret=""

        if (self.name != ""):
            ret=ret + " -N " + str(self.name)
        if (self.h_vmem != ""):
            ret=ret + " -l " + "h_vmem" + "=" + str(self.h_vmem)
        if (self.arch != ""):
            ret=ret + " -l " + "arch" + "=" + str(self.arch)
        if (self.tmpfree != ""):
            ret=ret + " -l " + "tmpfree" + "=" + str(self.tmpfree)
        if (self.h_cpu != ""):
            ret=ret + " -l " + "h_cpu" + "=" + str(self.h_cpu)
        if (self.h_rt != ""):
            ret=ret + " -l " + "h_rt" + "=" + str(self.h_rt)
        if (self.express != ""):
            ret=ret + " -l " + "express" + "=" + str(self.express)
        if (self.matlab != ""):
            ret=ret + " -l " + "matlab" + "=" + str(self.matlab)
        if (self.simulink != ""):
            ret=ret + " -l " + "simulink" + "=" + str(self.simulink)
        if (self.compiler != ""):
            ret=ret + " -l " + "compiler" + "=" + str(self.compiler)
        if (self.imagetb != ""):
            ret=ret + " -l " + "imagetb" + "=" + str(self.imagetb)
        if (self.opttb != ""):
            ret=ret + " -l " + "opttb" + "=" + str(self.opttb)
        if (self.stattb != ""):
            ret=ret + " -l " + "stattb" + "=" + str(self.stattb)
        if (self.sigtb != ""):
            ret=ret + " -l " + "sigtb" + "=" + str(self.sigtb)
        if (self.cplex != ""):
            ret=ret + " -l " + "cplex" + "=" + str(self.cplex)
        if (self.nicetohave != ""):
            ret=ret + " -l " + "nicetohave" + "=" + str(self.nicetohave)

        return ret

    def setNativeSpecification(self, x):
        """
        define python-style setter
        @param x: nativeSpecification string to be set
        @type x: string
        """

        self.__nativeSpecification=x

    nativeSpecification = property(getNativeSpecification, setNativeSpecification)


#TODO make read-only
#  def __getattribute__(self, name):
#
#    if (name == "nativeSpecification"):
#
#      ret=""
#
#      for attr in self.__dict__.keys():
#
#        print attr
#
#        value = self.__dict__[attr]
#        if (value!=""):
#          ret=ret + " -l " + attr + "=" + value
#
#      return ret
#
#
#    else:
#
#      return Job.__getattribute__(self, name)
#

#TODO this class will most likely disappear, soon.
class MethodJob:

    #TODO derive this from Job, unify!

    methodName=""
    obj=None
    args=()
    kwlist={}
    ret=None

    def __init__(self, m, args, kwlist={}):
        """

        @param m: method to execute
        @type m: method
        @param args: list of arguments
        @type args: list
        @param kwlist: keyword list
        @type kwlist: dict
        """

        self.methodName=m.im_func.func_name
        self.obj=m.im_self
        self.args=args
        self.kwlist=kwlist

    def execute(self):
        m=getattr(self.obj, self.methodName)
        self.ret=apply(m, self.args, self.kwlist)



class JobsThread (threading.Thread):
    """
    In case jobs are to be computed locally, a number of Jobs (possibly one)
    are assinged to one thread.
    """

    jobs=[]

    def __init__(self, jobs):
        """
        Constructor
        @param jobs: list of jobs
        @type jobs: list of Job objects
        """

        self.jobs = jobs
        threading.Thread.__init__(self)

    def run (self):
        """
        Executes each job in job list
        """
        for job in self.jobs:
            job.execute()



def _processJobsLocally(jobs, maxNumThreads=1):
    """
    Run jobs on local machine in a multithreaded manner, providing the same interface.
    NOT finished yet.

    @param jobs: list of jobs to be executed locally.
    @type jobs: list of Job objects
    @param maxNumThreads: defines the maximal number of threads to be used to process jobs
    @type maxNumThreads: integer
    """

    numJobs=len(jobs)

    print "number of jobs: ", numJobs

    #check if there are fewer jobs then allowed threads
    if (maxNumThreads >= numJobs):
        numThreads=numJobs
        jobsPerThread=1
    else:
        numThreads=maxNumThreads
        jobsPerThread=(numJobs/numThreads)+1

    print "number of threads: ", numThreads
    print "jobs per thread: ", jobsPerThread

    jobList=[]
    threadList=[]

    #assign jobs to threads
    #TODO use a queue here
    for (i, job) in enumerate(jobs):

        jobList.append(job)

        if ((i%jobsPerThread==0 and i!=0) or i==(numJobs-1)):
            #create new thread
            print "starting new thread"
            thread=JobsThread(jobList)
            threadList.append(thread)
            thread.start()
            jobList=[]


    #wait for threads to finish
    for thread in threadList:
        thread.join()

    return jobs


def _submitJobsToCluster(jobs):
    """
    Method used to send a list of jobs onto the cluster. Wait and forget.
    @param jobs: list of jobs to be executed
    @type jobs: list of Job objects
    """

    outdir=os.path.expanduser(TMPHOST)
    if not os.path.isdir(outdir):
        raise Exception()

    s=DRMAA.Session()
    s.init()
    joblist=[]
    fileNames=[]


    #fetch current path, and write to file
    pythonpath=sys.path
    pythonpath.append(os.getcwd())
    pythonpath.extend([os.path.expanduser(item) for item in PYTHONPATH])

    path_file = os.tempnam(outdir, "paths_") + ".bz2"
    #print 'Saving %s to %s.' % (pythonpath,path_file)
    save(path_file, pythonpath)
    

    for job in jobs:
        invars = os.tempnam(outdir, "pg") + ".bz2"
        job.name = invars.split(".")[-2].split("/")[-1]
        try:
            save(invars, job)
            fileNames.append(invars)
        except Exception, detail:
            print "error while pickling file: " + invars
            print detail



        jt = s.createJobTemplate()

        #TODO figure this out
        #jt.setEnvironment({"LD_LIBRARY_PATH": "/usr/local/sge/lib/lx26-amd64/"})


        jt.remoteCommand = os.path.expanduser(LOCATION)
        jt.args = [invars, path_file]
        jt.joinFiles=True

        #resources
        jt.setNativeSpecification(job.nativeSpecification)

        #set output directory
        #fn = path.replace(os.path.expanduser(TMPHOST), )
        jt.outputPath=":" + os.path.expanduser(TMPCLIENT)
        jobid = s.runJob(jt)
        print 'Your job %s has been submitted with id %s' % (job.name,jobid)

        #display file size
        # print os.system("du -h " + invars)

        joblist.append(jobid)

        #To clean things up, we delete the job template. This frees the memory DRMAA
        #set aside for the job template, but has no effect on submitted jobs.
        s.deleteJobTemplate(jt)


    #clean up path file
    #os.remove(path_file)

    return (s.getContact(),joblist,fileNames)



def _collectResultsFromCluster(sid,joblist,fileNames,wait=False):
    """
    Collect the results from the joblist
    """
    s=DRMAA.Session()
    s.init(sid)

    if wait:
        drmaaWait=DRMAA.Session.TIMEOUT_WAIT_FOREVER
    else:
        drmaaWait=DRMAA.Session.TIMEOUT_NO_WAIT

    s.synchronize(joblist, drmaaWait, True)
    print "success: all jobs finished"
    s.exit()

    #attempt to collect results
    retJobs=[]
    for fileName in fileNames:
        outvars = fileName + ".out"
        try:
            retJob=load(outvars)
            retJobs.append(retJob)
        except Exception, detail:
            print "error while unpickling file: " + outvars
            print "most likely there was an error during jobs execution"
            print detail

        #remove input file
        if retJob.cleanup:
            os.remove(outvars)

    return retJobs


def _processJobsOnCluster(jobs):
    """
    Use _submitJobsToCluster and _collectResultsFromCluster
    to run jobs and wait for the results.
    """

    (sid,joblist,fileNames)=_submitJobsToCluster(jobs)
    return _collectResultsFromCluster(sid,joblist,fileNames,wait=True)


def processJobs(jobs, local=False):
    """
    Director method to decide whether to run on cluster or locally
    """

    if (not local and drmaa_present):
        return _processJobsOnCluster(jobs)
    if (not local and not drmaa_present):
        return _processJobsLocally(jobs, maxNumThreads=3)
    else:
        return _processJobsLocally(jobs, maxNumThreads=3)


def submitJobs(jobs):
    return _submitJobsToCluster(jobs)

def collectJobs(sid,jobids,fileNames,wait=True):
    return _collectResultsFromCluster(sid,jobids,fileNames,wait)


#####################################################################
# Dealing with data
#####################################################################

def save(filename,myobj):
    """save the myobj to filename using pickle"""
    try:
        f = bz2.BZ2File(filename,'wb')
    except IOError, details:
        sys.stderr.write('File ' + filename + ' cannot be written\n')
        sys.stderr.write(details)
        return

    cPickle.dump(myobj,f,protocol=2)
    f.close()


def load(filename):
    """load the myobj from filename using pickle"""
    try:
        f = bz2.BZ2File(filename,'rb')
    except IOError, details:
        sys.stderr.write('File ' + filename + ' cannot be read\n')
        sys.stderr.write(details)
        return

    myobj = cPickle.load(f)
    f.close()
    return myobj


def create_shell(file_name):
    """
    creates temporary shell script
    """

    try:
        f=open(file_name, mode=700)

    except:
        print "muhahahaha"

################################################################
#      The following code will be executed on the cluster      #
################################################################

def runJob(pickleFileName, path_file):
    """
    Runs job which was pickled to a file called pickledFileName.
    Saved paths are loaded in advance from another Pickleded object
    called path_file.

    @param pickleFileName: filename of pickled Job object
    @type pickleFileName: string
    @param path_file: filename of pickled list of strings
    @type path_file: string
    """

    #print 'runJob(%s,%s)' % (pickleFileName,path_file)
    #restore pythonpath on cluster node
    saved_paths = load(path_file)
    sys.path.extend(saved_paths)

    inPath = pickleFileName
    job=load(inPath)

    job.execute()
    
    #remove input file
    if job.cleanup:
        os.remove(pickleFileName)


    outPath = pickleFileName + ".out"

    save(outPath, job)


class Usage(Exception):
    """
    Simple Exception for cmd-line user-interface.
    """

    def __init__(self, msg):
        """
        Constructor of simple Exception.
        @param msg: exception message
        @type msg: string
        """

        self.msg = msg



def main(argv=None):
    """
    Generic main

    @param argv: list of arguments
    @type argv: list of strings
    """


    if argv is None:
        argv = sys.argv

    try:
        try:

            opts, args = getopt.getopt(argv[1:], "h", ["help"])

            runJob(args[0], args[1])

        except getopt.error, msg:
            raise Usage(msg)


    except Usage, err:

        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"

        return 2



if __name__ == "__main__":
    main(sys.argv)


