import sys
import getopt
from pythongrid import KybJob, MethodJob, processJobs, Usage, submitJobs, collectJobs, getStatus
import time

#needs to be imported, such that module name can be referred to explicitly
import example


def makeJobs():
  '''
  Creates a list of Jobs.   
  '''

  jobs=[]
  j1 = KybJob(example.computeFaculty, [20])
  j1.h_vmem="300M"

  jobs.append(j1)

  #One needs to use the full identifier such that the module name is explicit.
  jobs.append(KybJob(example.computeFaculty, [30]))
  jobs.append(KybJob(example.computeFaculty, [40]))
  jobs.append(KybJob(example.computeFaculty, [50]))
  jobs.append(KybJob(example.computeFaculty, [50]))

  return jobs


def runExample():
  
  print "====================================="
  print "======= Local Multithreading ========"
  print "====================================="
  print ""
  print ""

  print "generating fuction jobs"

  functionJobs = makeJobs()

  print "output ret field in each job before multithreaded computation"
  for (i, job) in enumerate(functionJobs):
    print "Job with id: ", i, "- ret: ", job.ret

  print ""
  print "sending function jobs to local machine"
  print ""

  processedFunctionJobs = processJobs(functionJobs, local=2)

  print "ret fields AFTER execution on local machine"
  for (i, job) in enumerate(processedFunctionJobs):
    print "Job with id: ", i, "- ret: ", job.ret

  print ""
  print ""
  print "====================================="
  print "=======   Submit and Wait    ========"
  print "====================================="
  print ""
  print ""

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
  print "=======  Submit and Forget   ========"
  print "====================================="
  print ""
  print ""


  print 'demo session'
  myjobs = makeJobs()

  (sid,jobids)=submitJobs(myjobs)

  print 'checking whether finished'
  while not getStatus(sid,jobids):
    time.sleep(2)
        
  print 'collecting jobs'
  retjobs=collectJobs(sid,jobids,myjobs)
  print "ret fields AFTER execution on cluster"
  for (i, job) in enumerate(retjobs):
    print "Job with id: ", i, "- ret: ", job.ret

  print '--------------'



def computeFaculty(n):
  '''
  Dummy function for testing a regular (function) Job.
  '''

  ret=1

  for i in xrange(n):
    ret=ret*(i+1)

  return ret



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


