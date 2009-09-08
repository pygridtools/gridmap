from drmaa import *

s=Session()
print('DRMAA library v%s'%str(s.version))
# On LSF, it seems that session already initialized.
#s.initialize()
print('DRM system used: '+s.drmsInfo)
jt=JobTemplate()
jt.remoteCommand='/bin/sleep'
jt.args=['10']
jt.nativeSpecification=''
jname=s.runJob(jt)
# Explicitly kill the job
#s.control(jname, JobControlAction.TERMINATE)
jinfo=s.wait(jname, Session.TIMEOUT_WAIT_FOREVER)

# Show all the returned information
# print jinfo
if (jinfo.wasAborted):
    print('Job never ran')
if (jinfo.hasExited):
    print('Job exited using the following resources:')
    print jinfo.resourceUsage
if (jinfo.hasSignal):
    print('Job was signalled with '+jinfo.terminatedSignal)
s.exit()
                         

