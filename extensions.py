
_decodestatus = {
    DRMAA.Session.UNDETERMINED: 'process status cannot be determined',
    DRMAA.Session.QUEUED_ACTIVE: 'job is queued and active',
    DRMAA.Session.SYSTEM_ON_HOLD: 'job is queued and in system hold',
    DRMAA.Session.USER_ON_HOLD: 'job is queued and in user hold',
    DRMAA.Session.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
    DRMAA.Session.RUNNING: 'job is running',
    DRMAA.Session.SYSTEM_SUSPENDED: 'job is system suspended',
    DRMAA.Session.USER_SUSPENDED: 'job is user suspended',
    DRMAA.Session.DONE: 'job finished normally',
    DRMAA.Session.FAILED: 'job finished, but failed',
    }

def getStatus(joblist):
    s = DRMAA.Session()
    s.init()
    status_summary = {}.fromkeys(_decodestatus,0)
    for jobid in joblist:
        status_summary[s.getJobProgramStatus(jobid)] += 1

    for curkey in status_summary.keys():
        if status_summary[curkey]>0:
            print '%s: %d' % (_decodestatus[status_summary[curkey]],status_summary[curkey])



