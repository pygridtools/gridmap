import io_pickle

#functions that are used to create Jobs have to be defined in separate module
#from the one which containes the processJobs function.


class Test:
  '''
  Dummy class for testing MethodJob
  '''

  sideEffect="not set"

  def doSomething(self, a):
    print "executing test"
    self.sideEffect="sideeffect set"

    return a+a



def testFunction(string):
  '''
  Dummy function for testing a regular (function) Job.
  '''
  print string

  for i in xrange(10000000):
    i+1

  return string+string;
