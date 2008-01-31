class Test:

  sideEffect="not set"

  def doSomething(self, a):
    print "executing test"
    self.sideEffect="sideeffect set"

    return a+a


def testFunction(string):

  print string
  return string+string;


