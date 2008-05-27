import sys
import getopt
from pythongrid import Job, KybJob, MethodJob, processJobs, Usage


#NOTE: !!this is necessary in order to be able to use the full function identifier!!
import example


def makeMethodJobs():
  '''
  Creates a list of Method Jobs.
  '''

  myobj=Test()

  jobs=[]
  jobs.append(MethodJob(myobj.doSomething, ["aaaaaXXXXXXXXXXX"]))
  jobs.append(MethodJob(myobj.doSomething, ["bbbbbbbbbXXXXXXXXXYa"]))
  jobs.append(MethodJob(myobj.doSomething, ["ccccccccccXXXXXXXXX"]))

  return jobs


def makeJobs():
  '''
  Creates a list of regular (function) Jobs. One needs to use the full identifier
  such that the module name is saved as well.
  '''

  jobs=[]
  j1 = KybJob(example.testFunction, ["aaaaaXXXXXXXXXXX"])
  j1.h_vmem="300M"

  print "job #1: ", j1.nativeSpecification


  jobs.append(j1)

  jobs.append(Job(example.testFunction, ["bbbbbbbbbXXXXXXXXXYa"]))
  jobs.append(Job(example.testFunction, ["ccccccccccXXXXXXXXX"]))
  jobs.append(Job(example.testFunction, ["bbbbbbbbbXXXXXXXXXYa"]))
  jobs.append(Job(example.testFunction, ["ccccccccccXXXXXXXXX"]))

  return jobs


def runExample():

  print "====================================="
  print "generating fuction jobs"

  functionJobs = makeJobs()

  print "output ret field in each job before sending it onto the cluster"
  for (i, job) in enumerate(functionJobs):
    print "Job with id: ", i, "- ret: ", job.ret

  print ""
  print "sending function jobs to cluster"
  print ""

  processedFunctionJobs = processJobs(functionJobs)

  print "ret fields AFTER execution on cluster"
  for (i, job) in enumerate(processedFunctionJobs):
    print "Job with id: ", i, "- ret: ", job.ret


  print ""
  print ""

  print "====================================="
  print "======= Local Multithreading ========"
  print "====================================="

  print "generating fuction jobs"

  functionJobs = makeJobs()

  print "output ret field in each job before multithreaded computation"
  for (i, job) in enumerate(functionJobs):
    print "Job with id: ", i, "- ret: ", job.ret

  print ""
  print "sending function jobs to local machine"
  print ""

  processedFunctionJobs = processJobs(functionJobs, locally=2)

  print "ret fields AFTER execution on local machine"
  for (i, job) in enumerate(processedFunctionJobs):
    print "Job with id: ", i, "- ret: ", job.ret


  print "====================================="
 #print "generating method jobs"

  #methodJobs = makeMethodJobs()

  #print methodJobs[0].obj.sideEffect
  #print methodJobs[0].ret

  #processedMethodJobs = processJobs(methodJobs)

  #print processedMethodJobs[0].obj.sideEffect
  #print processedMethodJobs[0].ret


def testFunction(string):
  '''
  Dummy function for testing a regular (function) Job.
  '''
  print string

  for i in xrange(1000):
    i+1

  return string+string;



class Test:
  '''
  Dummy class for testing MethodJob. Might now work ATM, will be fixed, soon.
  '''

  sideEffect="not set"

  def doSomething(self, a):
    print "executing test"
    self.sideEffect="sideeffect set"

    return a+a



def main(argv=None):


  if argv is None:
    argv = sys.argv

  try:
    try:

      opts, args = getopt.getopt(argv[1:], "h", ["help"])

      runExample()

    except getopt.error, msg:
      raise Usage(msg)


  except Usage, err:

    print >>sys.stderr, err.msg
    print >>sys.stderr, "for help use --help"

    return 2



if __name__ == "__main__":

  main()
  #sys.exit(main())


