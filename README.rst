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

-  v0.11.1

   +  Made web front-end for job monitoring a separate script, ``gridmap_web``,
      since it can be used to talk to any ``JobMonitor`` instance. (Fixes #14)
   +  Fixed crash if a stalled job comes back from the dead (#15).
   +  Fixed crash if job's hostname is somehow not in white list and the job
      needs to be resubmitted (#16).
   +  Fixed crash from trying to set ``matplotlib`` back-end multiple times.
   +  Cleaned up some imports and removed some unused variables.

-  v0.11.0

   + Vastly more reliable job completion information thanks to switch back to
     using 0MQ for communication with worker nodes. No more unpickling
     exceptions because the SGE DRMAA implementation frequently liked to say
     jobs were finished when they were not.
   + Add back web monitor to report basic job status.
   + Switch to using custom fork of drmaa-python until
     drmaa-python/drmaa-python#4, which fixes Python 3 compatibility issues,
     gets merged.
   + Now creates temporary directory for storing log files if it doesn't
     exist.
   + Travis-CI SGE installation has been streamlined.
   + Switch to using sphinx and readthedocs for documentation.
   + Added detection of stalled jobs. GridMap will also automatically restart
     any jobs that appear stuck (up to 3 times by default), and email you a
     report describing their CPU and memory usage over time.

-  v0.10.3

   + Fix issue where ``clean_path`` wasn't being called on the working
     directory, which was causing ETS-specific issues.
   + Add a couple workarounds for issues with setting environment variables in
     Python 3.
   + Made examples into unit tests and added first attempt at getting Travis
     setup with SGE.

-  v0.10.2

   + Working directory is now correctly set for each job.
   + Simplified handling of environment variables. Should now all be passed on
     properly.

-  v0.10.1

   + Can now import ``JobException`` directly from ``gridmap`` package instead
     of having to import from ``gridmap.job``.

-  v0.10.0

   + Now raise a ``JobException`` instead of an ``Exception`` when one of the
     jobs has crashed.
   + Fixed potential pip installation issue from importing package for version
     number.

-  v0.9.9

   + Changed way job results are retrieved to be a bit more efficient in cases
     of errors.
   + All job metadata is now retrieved before job output is, which should
     hopefully alleviate issues where we can't get the metadata because its been
     flushed too quickly by the grid engine.

-  v0.9.8

   + Fixed a bug where only the first error was still showing because of an
     extra exception caused by job_output being undefined.
   + Fixed unhandled Exception with error code 24 (since somehow that is not an
     InvalidJobException, but just an Exception in drmaa-python).

-  v0.9.7

   + No longer dies with InvalidJobException when failing to retrieve job
     metadata from DRMAA service.
   + Now print all exceptions encountered for jobs submitted instead of just
     exiting after first one.
   + Die via exception instead of sys.exit when there were problems with some of
     the submitted jobs.

-  v0.9.6

   + Fixed bug where jobs were being aborted before they ran.

-  v0.9.5

   + Fixed bug where ``GRID_MAP_USE_MEM_FREE`` would only be interpretted as true if
     spelled 'True'.
   + Added documentation describing how to override constants.

-  v0.9.4

   +  Added support for overriding the default queue and other constants via
      environment variables. For example, to change the default queue, just set
      the environment variable ``GRID_MAP_DEFAULT_QUEUE``.
   +  Substantially more information is given about crashing jobs when we fail
      to unpickle the results from the Redis database.

-  v0.9.3

   +  Fixed serious bug where gridmap could not be imported in some instances.
   +  Refactored things a bit so there is no longer one large module with all of
      the code in it. (Doesn't change package interface)
