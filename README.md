Python Grid
-----------

A module to allow you to easily create jobs on the cluster directly from Python. You can directly map Python functions onto the cluster without needing to write any wrapper code yourself.

This is the ETS fork of an open source project originally hosted on Google Code. It's a bit more reliable (and easier to use) than the original version, because we use a MySQL database for storing the inputs/outputs for each job instead of the ZeroMQ-based method they were using.

For some examples of how to use it, check out example_map_reduce.py (for a simple example of how you can map a function onto the cluster) and example_manual.py (for an example of how you can create list of jobs yourself).

For complete documentation go [here](../blob/master/doc/index.html).
