#! /usr/bin/env python

import DRMAA
import os
import sys
import getopt
import io_pickle
import random
import types
import thread



class Job:

  f=None
  args=()
  kwlist={}
  ret=None


  def __init__(self, f, args, kwlist={}, additionalpath=[]):
 
    self.f=f
    self.args=args
    self.kwlist=kwlist

    #fetch current path
    self.pythonpath=sys.path
    self.pythonpath.append(os.getcwd())

    #TODO: set additional path from a config file
    self.pythonpath.extend(additionalpath)

  def execute(self):
    self.ret=apply(self.f,self.args,self.kwlist)


#TODO derive this from Job, unify!
class MethodJob:

  methodName=""
  obj=None
  args=()
  kwlist={}
  ret=None

  def __init__(self, m, args, kwlist={}):
 
    self.methodName=m.im_func.func_name
    self.obj=m.im_self
    self.args=args
    self.kwlist=kwlist

  def execute(self):
    m=getattr(self.obj, self.methodName)
    self.ret=apply(m,self.args,self.kwlist)



def createFileName():
  return "pickle_" + randomString(10) + ".bz2"



def randomString(length):
  alphabet=['a','b','c','d']

  string = [random.choice(alphabet) for j in range(length)]
  string = "".join(string)

  return string



def processJobsLocally(jobs, numThreads=1):
  """Run jobs on local machine in a multithreaded manner"""
  
  for job in jobs:


    #TODO implement
    print "start new thread"


def processJobs(jobs):
  """Submit an array job.
  Note, need file called sleeper.sh in home directory. An example:
  echo 'Hello World $1'
  """


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


  #collect results
  for fileName in fileNames:
 
    path = fileName + ".out"

    print path

    retJob=io_pickle.load(path)

    retJobs.append(retJob)


  #clean up path file
  os.remove(path_file)
  
  return retJobs
    



################################################################
#      The following code will be executed on the cluster      #
################################################################


def runJob(pickleFileName, path_file):
  """Runs job which was pickled to a file called pickledFileName
  """
 
  dir=""

  #restore pythonpath on cluster node
  saved_paths = io_pickle.load(path_file)
  sys.path.extend(saved_paths)


  inPath = dir + pickleFileName
  job=io_pickle.load(inPath)

  job.execute()

  outPath = dir + pickleFileName + ".out"
  
  io_pickle.save(outPath, job)

  #TODO clean up files
  

class Usage(Exception):

  def __init__(self, msg):

    self.msg = msg

  

def main(argv=None):


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


