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

See `GitHub releases <https://github.com/EducationalTestingService/gridmap/releases>`__.
