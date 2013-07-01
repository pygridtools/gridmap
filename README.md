Python Grid
-----------

A module to allow you to easily create jobs on the cluster directly from Python. You can directly map Python functions onto the cluster without needing to write any wrapper code yourself.

This is the ETS fork of an open source project originally hosted on Google Code. It's a lot simpler than the original version, because we use a Redis database for storing the inputs/outputs for each job instead of the ZeroMQ-based method they were using. The main benefit of this approach is you never run into issues with exceeding the message length when you're parallelizing a huge job.

For some examples of how to use it, check out example_map_reduce.py (for a simple example of how you can map a function onto the cluster) and example_manual.py (for an example of how you can create list of jobs yourself).

For complete documentation go [here](http://htmlpreview.github.io/?http://github.com/EducationalTestingService/pythongrid/blob/master/doc/index.html).

*NOTE*: You cannot use Python Grid on a machine that is not allowed to submit jobs (e.g., slave nodes).

### Requirements ###

* [redis-py](https://github.com/andymccurdy/redis-py)
* [drmaa-python](http://drmaa-python.github.io/)
* [six](http://pythonhosted.org/six/)
* Python 2.6+

### Recommended ###
* [hiredis](https://pypi.python.org/pypi/hiredis)

### License ###
GPLv3
