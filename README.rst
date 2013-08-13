Grid Map
-----------

A module to allow you to easily create jobs on the cluster directly from
Python. You can directly map Python functions onto the cluster without
needing to write any wrapper code yourself.

This is the ETS fork of an older project called Python Grid. It's a lot
simpler than the original version, because we use a Redis database for
storing the inputs/outputs for each job instead of the ZeroMQ-based
method they were using. The main benefit of this approach is you never
run into issues with exceeding the message length when you're
parallelizing a huge job.

For some examples of how to use it, check out map\_reduce.py
(for a simple example of how you can map a function onto the cluster)
and manual.py (for an example of how you can create list of
jobs yourself) in the examples folder.

For complete documentation go
`here <http://htmlpreview.github.io/?http://github.com/EducationalTestingService/gridmap/blob/master/doc/index.html>`__.

*NOTE*: You cannot use Grid Map on a machine that is not allowed to
submit jobs (e.g., slave nodes).

Requirements
~~~~~~~~~~~~

-  `redis-py <https://github.com/andymccurdy/redis-py>`__
-  `drmaa-python <http://drmaa-python.github.io/>`__
-  Python 2.6+

Recommended
~~~~~~~~~~~

-  `hiredis <https://pypi.python.org/pypi/hiredis>`__

License
~~~~~~~

-  GPLv3

Changelog
~~~~~~~~~

-  0.9.9

   + Changed way job results are retrieved to be a bit more efficient in cases of errors.
   + All job metadata is now retrieved before job output is, which should hopefully alleviate issues where we can't get the metadata because its been flushed too quickly by the grid engine.

-  0.9.8

   + Fixed a bug where only the first error was still showing because of an extra exception caused by job_output being undefined.
   + Fixed unhandled Exception with error code 24 (since somehow that is not an InvalidJobException, but just an Exception in drmaa-python).

-  0.9.7

   + No longer dies with InvalidJobException when failing to retrieve job metadata from DRMAA service.
   + Now print all exceptions encountered for jobs submitted instead of just exiting after first one.
   + Die via exception instead of sys.exit when there were problems with some
     of the submitted jobs.

-  0.9.6

   + Fixed bug where jobs were being aborted before they ran.

-  0.9.5

   + Fixed bug where GRID_MAP_USE_MEM_FREE would only be interpretted as true if spelled 'True'.
   + Added documentation describing how to override constants.

-  0.9.4

   +  Added support for overriding the default queue and other constants via environment variables. For example, to change the default queue, just set the environment variable GRID_MAP_DEFAULT_QUEUE.
   +  Substantially more information is given about crashing jobs when we fail to unpickle the results from the Redis database.

-  0.9.3

   +  Fixed serious bug where gridmap could not be imported in some instances.
   +  Refactored things a bit so there is no longer one large module with all of the code in it. (Doesn't change package interface)



.. image:: https://d2weczhvl823v0.cloudfront.net/EducationalTestingService/gridmap/trend.png
   :alt: Bitdeli badge
   :target: https://bitdeli.com/free



.. image:: https://d2weczhvl823v0.cloudfront.net/EducationalTestingService/gridmap/trend.png
   :alt: Bitdeli badge
   :target: https://bitdeli.com/free

