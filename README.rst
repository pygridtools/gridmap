GridMap
-----------

.. image:: https://travis-ci.org/EducationalTestingService/gridmap.png
   :target: https://travis-ci.org/EducationalTestingService/gridmap
   :alt: Travis build status


.. image:: https://coveralls.io/repos/EducationalTestingService/gridmap/badge.png
  :target: https://coveralls.io/r/EducationalTestingService/gridmap
  :alt: Test coverage

.. image:: https://pypip.in/d/gridmap/badge.png
   :target: https://crate.io/packages/gridmap
   :alt: PyPI downloads

.. image:: https://pypip.in/v/gridmap/badge.png
   :target: https://crate.io/packages/gridmap
   :alt: Latest version on PyPI

.. image:: https://d2weczhvl823v0.cloudfront.net/EducationalTestingService/gridmap/trend.png
   :target: https://bitdeli.com/free
   :alt: Bitdeli badge


A package to allow you to easily create jobs on the cluster directly from
Python. You can directly map Python functions onto the cluster without needing
to write any wrapper code yourself.

This is the ETS fork of an older project called Python Grid. Unlike the older
version,  it is Python 2/3 compatible. Another major difference is that you can
change the  configuration via environment variables instead of having to modify
a Python file in your ``site-packages`` directory. We've also removed some cruft
and improved the reliability of some of the features.

For some examples of how to use it, check out ``map_reduce.py`` (for a simple
example of how you can map a function onto the cluster) and ``manual.py`` (for
an example of how you can create list of jobs yourself) in the examples folder.

For complete documentation `read the docs <http://gridmap.readthedocs.org>`__.

*NOTE*: You cannot use GridMap on a machine that is not allowed to submit jobs
(e.g., slave nodes).

Requirements
~~~~~~~~~~~~

-  `drmaa <https://github.com/drmaa-python/drmaa-python>`__
-  `pyzmq <https://github.com/zeromq/pyzmq>`__
-  Python 2.7+

License
~~~~~~~

-  GPLv3

Changelog
~~~~~~~~~

-  0.11.0
   
   + 

-  0.10.3

   + Fix issue where ``clean_path`` wasn't being called on the working
     directory, which was causing ETS-specific issues.
   + Add a couple workarounds for issues with setting environment variables in
     Python 3.
   + Made examples into unit tests and added first attempt at getting Travis
     setup with SGE.

-  0.10.2

   + Working directory is now correctly set for each job.
   + Simplified handling of environment variables. Should now all be passed on
     properly.

-  0.10.1

   + Can now import ``JobException`` directly from ``gridmap`` package instead
     of having to import from ``gridmap.job``.

-  0.10.0

   + Now raise a ``JobException`` instead of an ``Exception`` when one of the
     jobs has crashed.
   + Fixed potential pip installation issue from importing package for version
     number.

-  0.9.9

   + Changed way job results are retrieved to be a bit more efficient in cases
     of errors.
   + All job metadata is now retrieved before job output is, which should
     hopefully alleviate issues where we can't get the metadata because its been
     flushed too quickly by the grid engine.

-  0.9.8

   + Fixed a bug where only the first error was still showing because of an
     extra exception caused by job_output being undefined.
   + Fixed unhandled Exception with error code 24 (since somehow that is not an
     InvalidJobException, but just an Exception in drmaa-python).

-  0.9.7

   + No longer dies with InvalidJobException when failing to retrieve job
     metadata from DRMAA service.
   + Now print all exceptions encountered for jobs submitted instead of just
     exiting after first one.
   + Die via exception instead of sys.exit when there were problems with some of
     the submitted jobs.

-  0.9.6

   + Fixed bug where jobs were being aborted before they ran.

-  0.9.5

   + Fixed bug where ``GRID_MAP_USE_MEM_FREE`` would only be interpretted as true if
     spelled 'True'.
   + Added documentation describing how to override constants.

-  0.9.4

   +  Added support for overriding the default queue and other constants via
      environment variables. For example, to change the default queue, just set
      the environment variable ``GRID_MAP_DEFAULT_QUEUE``.
   +  Substantially more information is given about crashing jobs when we fail
      to unpickle the results from the Redis database.

-  0.9.3

   +  Fixed serious bug where gridmap could not be imported in some instances.
   +  Refactored things a bit so there is no longer one large module with all of
      the code in it. (Doesn't change package interface)
