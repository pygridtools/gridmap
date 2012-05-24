"""configuration file for pythongrid"""
import os

CFG = {}

#default python path
CFG['PYTHONPATH'] = os.environ['PYTHONPATH']
CFG['PYGRID']     = "/fml/ag-raetsch/home/cwidmer/svn/tools/python/pythongrid/pythongrid.py"
CFG['TEMPDIR']    = "/fml/ag-raetsch/home/cwidmer/tmp/"
CFG['node_blacklist']    = []

# error messages
CFG['SMTPSERVER'] = "mailhost.tuebingen.mpg.de"
CFG['ERROR_MAIL_SENDER'] = "cwidmer@tuebingen.mpg.de"
CFG['ERROR_MAIL_RECIPIENT'] = "ckwidmer@gmail.com"
CFG['MAX_MSG_LENGTH'] = 5000


# under the hood

# how much time can pass between heartbeats, before
# job is assummed to be dead in seconds
CFG['MAX_TIME_BETWEEN_HEARTBEATS'] = 90

# factor by which to increase the requested memory
# if an out of memory event was detected. 
CFG['OUT_OF_MEM_INCREASE'] = 2.0

# defines how many times can a particular job can die, 
# before we give up
CFG['NUM_RESUBMITS'] = 3

# check interval: how many seconds pass before we check
# on the status of a particular job in seconds
CFG['CHECK_FREQUENCY'] = 15

# heartbeat frequency: how many seconds pass before jobs
# on the cluster send back heart beats to the submission 
# host
CFG['HEARTBEAT_FREQUENCY'] = 10


#paths on cluster file system
# TODo set this in configuration file

# location of pythongrid.py on cluster file system
# ToDO set this in configuration file

#PPATH = reduce(lambda x,y: x+':'+y, PYTHONPATH)
#print PPATH
#os.environ['PYTHONPATH'] = PPATH
#sys.path.extend(PYTHONPATH)

