#! /usr/bin/env python

import DRMAA
import os
import sys
import getopt
import io_pickle
import random
import types
import threading



class Job (object):
  '''
  Central entity that wrappes a function and its data. Basically,
  a job consists of a function, its argument list, its
  keyword list and a field "ret" which is filled, when
  the execute method gets called
  '''

  f=None
  args=()
  kwlist={}
  ret=None

  nativeResources=""


  def __init__(self, f, args, kwlist={}, additionalpaths=[]):
    '''
    constructor of Job

    @param f: a function, which should be executed.
    @type f: function
    @param args: argument list of function f
    @type args: list
    @param kwlist: dictionary of keyword arguments
    @type kwlist: dict
    @param additionalpaths: additional paths for use on cluster nodes
    @type additionalpaths: list of strings
    '''

    self.f=f
    self.args=args
    self.kwlist=kwlist

    #fetch current path
    self.pythonpath=sys.path
    self.pythonpath.append(os.getcwd())


    #TODO: set additional path from a config file
    #self.pythonpath.extend(additionalpaths)


  def execute(self):
    '''
    Executes function f with given arguments and writes return value to field ret.
    Input data is removed after execution to save space.
    '''
    self.ret=apply(self.f,self.args,self.kwlist)

    #TODO make this optional!
    #remove input data
    self.args=[]
    self.kwlist={}



class KybJob(Job):
  '''
  Specialization of generic Job that provides an interface to Kyb-specific options.
  Not quite finished yet, interface will most likely change in the near future. (march 2008)
  '''

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

  def getNativeResources(self):
    '''
    define python-style getter
    '''

    ret=""
    #TODO
    value="TODO"

    if (value!=""):
      ret=ret + " -l " + "h_vmem" + "=" + self.h_vmem

    return ret

  def setNativeResources(self, x):
    '''
    define python-style setter
    @param x: nativeResources string to be set
    @type x: string
    '''

    self.__nativeResources=x

  nativeResources=property(getNativeResources, setNativeResources)


#  def __getattribute__(self, name):
#
#    if (name == "nativeResources"):
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
    '''

    @param m: method to execute
    @type m: method
    @param args: list of arguments
    @type args: list
    @param kwlist: keyword list
    @type kwlist: dict
    '''

    self.methodName=m.im_func.func_name
    self.obj=m.im_self
    self.args=args
    self.kwlist=kwlist

  def execute(self):
    m=getattr(self.obj, self.methodName)
    self.ret=apply(m,self.args,self.kwlist)



class JobsThread (threading.Thread):
  '''
  In case jobs are to be computed locally, a number of Jobs (possibly one)
  are assinged to one thread.
  '''

  jobs=[]

  def __init__(self, jobs):
    '''
    Constructor
    @param jobs: list of jobs
    @type jobs: list of Job objects
    '''

    self.jobs = jobs
    threading.Thread.__init__(self)

  def run (self):
    '''
    Executes each job in job list
    '''
    for job in self.jobs:
      job.execute()



def createFileName():
  '''
  Generates random filename.
  '''
  return "pickle_" + randomString(10) + ".bz2"



def randomString(length):
  '''
  Generates random string of specified length. Only a 4-letter alphabet is used atm.
  @param length: length of random string
  @type length: integer
  '''
  alphabet=['a','b','c','d']

  string = [random.choice(alphabet) for j in range(length)]
  string = "".join(string)



  return string



def processJobsLocally(jobs, maxNumThreads=1):
  '''
  Run jobs on local machine in a multithreaded manner, providing the same interface.
  NOT finished yet.

  @param jobs: list of jobs to be executed locally.
  @type jobs: list of Job objects
  @param maxNumThreads: defines the maximal number of threads to be used to process jobs
  @type maxNumThreads: integer
  '''

  numThreads=1
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

    if ((i%jobsPerThread==0 and i!=0) or i==(numJobs-1) ):
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




def processJobs(jobs):
  '''
  Method used to send a list of jobs onto the cluster. Method will wait for all jobs to finish and return
  the list of jobs with the respective ret field set.

  @param jobs: list of jobs to be executed
  @type jobs: list of Job objects
  '''

  dir=os.path.expanduser("~/")

  s=DRMAA.Session()
  s.init()


  joblist=[]

  fileNames=[]


  #set paths

  #fetch current path
  pythonpath=sys.path
  pythonpath.append(os.getcwd())
  #TODO: set additional path from a config file
  #pythonpath.extend(additionalpath)

  path_file= dir + "pythongrid_paths_" + randomString(5) + ".pkl"

  #write paths to a separate file
  io_pickle.save(path_file, pythonpath)


  for job in jobs:

    fileName = createFileName()

    path = dir + fileName
    print "path: " + path

    try:
      io_pickle.save(path, job)
      fileNames.append(path)
    except Exception, detail:
      print "error while pickling file: " + dir + fileName
      print detail



    print 'Creating job template'
    jt = s.createJobTemplate()

    #TODO FILENAME!
    command=os.path.expanduser('~/svn/tools/python/pythongrid/pythongrid_runner.sh')

    jt.remoteCommand = command
    jt.args = [path, path_file]
    jt.joinFiles=True

    #TODO SAVEDIR
    homeDir = os.path.expanduser("~/")
    jt.outputPath=":" + homeDir
    jobid = s.runJob(jt)
    print 'Your job has been submitted with id ' + str(jobid)

    joblist.append(jobid)

    #To clean things up, we delete the job template. This frees the memory DRMAA
    #set aside for the job template, but has no effect on submitted jobs.
    #s.deleteJobTemplate(jt)


  #wait for jobs to finish
  s.synchronize(joblist, DRMAA.Session.TIMEOUT_WAIT_FOREVER, True)

  print "success: all jobs finished"

  s.exit()

  retJobs=[]


  #attempt to collect results
  for fileName in fileNames:

    path = fileName + ".out"

    print path

    try:
      retJob=io_pickle.load(path)
      retJobs.append(retJob)
    except Exception, detail:
      print "error while unpickling file: " + path
      print "most likely there was an error when executing a Job"
      print detail





  #clean up path file
  os.remove(path_file)

  return retJobs


################################################################
#      The following code will be executed on the cluster      #
################################################################

def runJob(pickleFileName, path_file):
  '''
  Runs job which was pickled to a file called pickledFileName.
  Saved paths are loaded in advance from another Pickleded object
  called path_file.

  @param pickleFileName: filename of pickled Job object
  @type pickleFileName: string
  @param path_file: filename of pickled list of strings
  @type path_file: string
  '''

  dir=""

  #restore pythonpath on cluster node
  saved_paths = io_pickle.load(path_file)
  sys.path.extend(saved_paths)


  inPath = dir + pickleFileName
  job=io_pickle.load(inPath)

  job.execute()

  #remove input file
  os.remove(dir + pickleFileName)

  outPath = dir + pickleFileName + ".out"

  io_pickle.save(outPath, job)


class Usage(Exception):
  '''
  Simple Exception for cmd-line user-interface.
  '''

  def __init__(self, msg):
    '''
    Constructor of simple Exception.
    @param msg: exception message
    @type msg: string
    '''

    self.msg = msg



def main(argv=None):
  '''
  Generic main

  @param argv: list of arguments
  @type argv: list of strings
  '''


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

  main()
  #sys.exit(main())


