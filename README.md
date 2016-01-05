#GridMap

This is a WIP fork of the original gridmap project. This was created to achieve compatability
with the TORQUE and PBS grid engine.

### Requirements
Jobs can only be submitted from nodes than are allowed to do that (i.e they can run 'qsub')

A couple of environment variables need to be set in order to work.

ERROR_MAIL_RECIPIENT = *your email address*

DRMAA_LIBRARY_PATH = *like pbs_drmaa/libs/libdrmaa.so for pbs*

DEFAULT_TEMP_DIR = *scratch space on the computing nodes*

SEND_ERROR_MAIL=TRUE

SMTP_SERVER = *your smtp server. like: unimail.tu-dortmund.de*


### Python Requirements


-  drmaa <https://github.com/drmaa-python/drmaa-python>
-  psutil <https://github.com/giampaolo/psutil>
-  pyzmq <https://github.com/zeromq/pyzmq>
-  Python 3.4+
