import io_pickle

class Test:

  sideEffect="not set"

  def doSomething(self, a):
    print "executing test"
    self.sideEffect="sideeffect set"

    return a+a


def testFunction(string):

  print string
  return string+string;


def findProblem():
  
  f=testFunction
  print f.__module__
  print f.func_code

  io_pickle.save("/tmp/mytest.bzip", f)
