Python Grid
-----------

A module to allow you to easily create jobs on the cluster directly from Python. You can directly map Python functions onto the cluster without needing to write any wrapper code yourself.

This is a fork of a fairly unmaintained project on Google Code. It's much more reliable (and easier to use) than the original version, because we use a MySQL database for storing the inputs/outputs for each job instead of the crazy message-passing method they were using.

For some examples of how to use it, check out example_map_reduce.py (for a simple example of how you can map a function onto the cluster) and example_manual.py (for an example of how you can create list of jobs yourself).

The PyDoc is available here: http://nlp.research.ets.org/~dnapolitano/pythongrid/
