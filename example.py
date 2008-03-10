import sys
import getopt
from pythongrid import Job, MethodJob, processJobs, Usage
from example_fun import Test, testFunction


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
  Creates a list of regular (function) Jobs.
  '''

  jobs=[]
  jobs.append(Job(testFunction, ["aaaaaXXXXXXXXXXX"]))
  jobs.append(Job(testFunction, ["bbbbbbbbbXXXXXXXXXYa"]))
  jobs.append(Job(testFunction, ["ccccccccccXXXXXXXXX"]))

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


  print "====================================="
  #print "generating method jobs"

  #methodJobs = makeMethodJobs()

  #print methodJobs[0].obj.sideEffect
  #print methodJobs[0].ret

  #processedMethodJobs = processJobs(methodJobs)

  #print processedMethodJobs[0].obj.sideEffect
  #print processedMethodJobs[0].ret



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


