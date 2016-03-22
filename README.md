#GridMap

This is a WIP fork of the original gridmap project. This was created to achieve compatability
with the TORQUE and PBS grid engine.

### Requirements
Jobs can only be submitted from nodes than are allowed to do that (i.e they can run 'qsub')

A couple of environment variables need to be set in order to work.

ERROR_MAIL_RECIPIENT = *your email address*

export DRMAA_LIBRARY_PATH = *like pbs_drmaa/libs/libdrmaa.so for pbs*

export DEFAULT_TEMP_DIR="/local/$USER/"

export USE_MEM_FREE=TRUE

export SMTP_SERVER="unimail.tu-dortmund.de"

export ERROR_MAIL_RECIPIENT="your.email@address.com"

export ERROR_MAIL_SENDER="torque@hpc-main3.phido.physik.tu-dortmund.de"

export SEND_ERROR_MAIL=TRUE

### Python Requirements


-  drmaa <https://github.com/drmaa-python/drmaa-python>
-  psutil <https://github.com/giampaolo/psutil>
-  pyzmq <https://github.com/zeromq/pyzmq>
-  Python 3.4+
