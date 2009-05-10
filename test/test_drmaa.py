from drmaa import *

s=Session()
print('DRMAA library v%s'%str(s.version))
# On LSF, it seems that session already initialized.
#s.initialize()
print('DRM system used: '+s.drmsInfo)
jt=JobTemplate()
jt.remoteCommand='/bin/sleep'
jt.args=['10']
jt.nativeSpecification='-q ether.s'
jname=s.runJob(jt)
s.control(jname, JobControlAction.TERMINATE)
jinfo=s.wait(jname, Session.TIMEOUT_WAIT_FOREVER)
if (jinfo.wasAborted):
    print('Job never ran')
if (jinfo.hasExited):
    print('Job exited with '+jinfo.exitStatus)
if (jinfo.hasSignal):
    print('Job was signalled with '+jinfo.terminatingSignal)
s.exit()
                         
print jinfo
