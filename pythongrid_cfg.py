"""configuration file for pythongrid"""
import os


def get_white_list():
    """
    parses output of qstat -f to get list of nodes

    WARNING: possibly MPI Tuebingen specific: make sure this works!
    """

    try:
        qstat = os.popen("qstat -f")

        node_names = []
        # norm_loads = []

        for line in qstat:

            # we kick out all old nodes, node1XX
            if line.startswith("all.q@") or line.startswith("nlp.q"):
                tokens = line.strip().split()
                node_name = tokens[0]

                if len(tokens) == 6:
                    continue
            
                # slots = float(tokens[2].split("/")[2])
                # cpu_load = float(tokens[3])

                # norm_load = cpu_load/slots 

                node_names.append(node_name)
                # norm_loads.append(norm_load)

        qstat.close()

        return node_names

    except Exception, details:
        print "getting whitelist failed", details
        return ""


CFG = {}

#default python path
CFG['PYTHONPATH'] = os.environ['PYTHONPATH']
CFG['PYGRID']     = "/home/research/dblanchard/pythongrid/pythongrid.py"
#CFG['PYGRID']     = "/opt/python/2.7/lib/python2.7/site-packages/pythongrid/pythongrid.py"
CFG['TEMPDIR']    = "~/tmp/"
if not os.path.exists(os.path.expanduser(CFG['TEMPDIR'])):
    os.makedirs(os.path.expanduser(CFG['TEMPDIR']))

# error emails
CFG['SMTPSERVER'] = "research.ets.org"
CFG['ERROR_MAIL_SENDER'] = os.environ['USER'] + "@ets.org"
CFG['ERROR_MAIL_RECIPIENT'] = os.environ['USER'] + "@ets.org"
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

# white-list of nodes to use
CFG['WHITELIST'] = get_white_list()

# black-list of nodes
CFG['BLACKLIST'] = []

# remove black-list from white-list
for node in CFG['BLACKLIST']:
    CFG['WHITELIST'].remove(node)

#paths on cluster file system
# TODo set this in configuration file

# location of pythongrid.py on cluster file system
# ToDO set this in configuration file

#PPATH = reduce(lambda x,y: x+':'+y, PYTHONPATH)
#print PPATH
#os.environ['PYTHONPATH'] = PPATH
#sys.path.extend(PYTHONPATH)

if __name__ == '__main__':

    for key, value in CFG.items():
        print '#'*30
        print key, value

