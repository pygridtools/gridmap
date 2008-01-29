#! /usr/bin/env python

import DRMAA
import os
import sys
import getopt
import io_pickle
import random
import types


#test code
######################################################

class Test:

  sideEffect="not set"

  def doSomething(self, a):
    print "executing test"
    self.sideEffect="sideeffect set"

    return a+a


def makeMethodJobs():

  myobj=Test()

  jobs=[]
  jobs.append(MethodJob(myobj.doSomething, ["aaaaaXXXXXXXXXXX"]))
  jobs.append(MethodJob(myobj.doSomething, ["bbbbbbbbbXXXXXXXXXYa"]))
  jobs.append(MethodJob(myobj.doSomething, ["ccccccccccXXXXXXXXX"]))

  return jobs


def makeJobs():

  jobs=[]
  jobs.append(Job(test, ["aaaaaXXXXXXXXXXX"]))
  jobs.append(Job(test, ["bbbbbbbbbXXXXXXXXXYa"]))
  jobs.append(Job(test, ["ccccccccccXXXXXXXXX"]))

  return jobs



def test(string):

  print string
  return string+string;


######################################################

class Job:

  f=None
  args=()
  kwlist={}
  ret=None

  def __init__(self, f, args, kwlist={}):
 
    self.f=f
    self.args=args
    self.kwlist=kwlist

  def execute(self):
    self.ret=apply(self.f,self.args,self.kwlist)


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
  #TODO: do some hashing here
  alphabet=['a','b','c','d']

  string = [random.choice(alphabet) for j in range(10)]
  string = "".join(string)

  return "pickle_" + string + ".bz2"



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
    jt.args = [path]
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

  
  return retJobs
    


def runJob(pickleFileName):
  """Runs job which was pickled to a file called pickledFileName
  """
  
  dir=""

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

      runJob(args[0])

    except getopt.error, msg:
      raise Usage(msg)


  except Usage, err:

    print >>sys.stderr, err.msg
    print >>sys.stderr, "for help use --help"

    return 2

  

if __name__ == "__main__":

  main()
  #sys.exit(main())


