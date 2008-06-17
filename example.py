import sys
import getopt
from pythongrid import KybJob, Usage
from pythongrid import process_jobs, submit_jobs, collect_jobs, get_status
import time

#needs to be imported, such that module name can be referred to explicitly
import example


def makeJobs():
    """
    Creates a list of Jobs.
    """
    inputvec = [[20], [30], [40], [50]]
    print 'print computing the factorials of %s' % str(inputvec)
    jobs=[]
    j1 = KybJob(example.computeFactorial, inputvec[0])
    j1.h_vmem="300M"

    jobs.append(j1)

    # One needs to use the full identifier
    # such that the module name is explicit.
    jobs.append(KybJob(example.computeFactorial, inputvec[1]))
    jobs.append(KybJob(example.computeFactorial, inputvec[2]))
    jobs.append(KybJob(example.computeFactorial, inputvec[3]))

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
        print "Job #", i, "- ret: ", job.ret

    print ""
    print "executing jobs on local machine"
    print ""

    processedFunctionJobs = process_jobs(functionJobs, local=True)

    print "ret fields AFTER execution on local machine"
    for (i, job) in enumerate(processedFunctionJobs):
        print "Job #", i, "- ret: ", job.ret

    print ""
    print ""
    print "====================================="
    print "=======     Submit and Wait        ========"
    print "====================================="
    print ""
    print ""

    functionJobs = makeJobs()

    print "output ret field in each job before sending it onto the cluster"
    for (i, job) in enumerate(functionJobs):
        print "Job #", i, "- ret: ", job.ret

    print ""
    print "sending function jobs to cluster"
    print ""

    processedFunctionJobs = process_jobs(functionJobs)

    print "ret fields AFTER execution on cluster"
    for (i, job) in enumerate(processedFunctionJobs):
        print "Job #", i, "- ret: ", job.ret


    print ""
    print ""
    print "====================================="
    print "=======    Submit and Forget     ========"
    print "====================================="
    print ""
    print ""


    print 'demo session'
    myjobs = makeJobs()

    (sid, jobids) = submit_jobs(myjobs)

    print 'checking whether finished'
    while not get_status(sid, jobids):
        time.sleep(7)
    print 'collecting jobs'
    retjobs = collect_jobs(sid, jobids, myjobs)
    print "ret fields AFTER execution on cluster"
    for (i, job) in enumerate(retjobs):
        print "Job #", i, "- ret: ", job.ret

    print '--------------'


def computeFactorial(n):
    """
    computes factorial of n
    """
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
