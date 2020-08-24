GridMap
-----------

.. image:: https://img.shields.io/travis/pygridtools/gridmap/stable.svg
   :alt: Build status
   :target: https://travis-ci.org/pygridtools/gridmap

.. image:: https://img.shields.io/coveralls/pygridtools/gridmap/stable.svg
    :target: https://coveralls.io/r/pygridtools/gridmap

.. image:: https://img.shields.io/pypi/dm/gridmap.svg
   :target: https://warehouse.python.org/project/gridmap/
   :alt: PyPI downloads

.. image:: https://img.shields.io/pypi/v/gridmap.svg
   :target: https://warehouse.python.org/project/gridmap/
   :alt: Latest version on PyPI

.. image:: https://img.shields.io/pypi/l/gridmap.svg
   :alt: License

A package to allow you to easily create jobs on the cluster directly from
Python. You can directly map Python functions onto the cluster without needing
to write any wrapper code yourself.

This is the ETS fork of an older project called `Python Grid <https://github.com/pygridtools/pythongrid>`__. Unlike the older
version, it is Python 2/3 compatible. Another major difference is that you can
change the configuration via environment variables instead of having to modify
a Python file in your ``site-packages`` directory. We've also fixed some bugs.

For some examples of how to use it, check out ``map_reduce.py`` (for a simple
example of how you can map a function onto the cluster) and ``manual.py`` (for
an example of how you can create list of jobs yourself) in the examples folder.

For complete documentation `read the docs <http://gridmap.readthedocs.org>`__.

*NOTE*: You cannot use GridMap on a machine that is not allowed to submit jobs
(e.g., slave nodes).

Requirements
~~~~~~~~~~~~

-  `cloudpickle <https://github.com/cloudpipe/cloudpickle>`__
-  `drmaa <https://github.com/drmaa-python/drmaa-python>`__
-  `psutil <https://github.com/giampaolo/psutil>`__
-  `pyzmq <https://github.com/zeromq/pyzmq>`__
-  Python 2.7+

Acknowledgments
~~~~~~~~~~~~~~~

Thank you to `Max-Planck-Society <http://www.mpg.de/en>`__ and
`Educational Testing Service <https://github.com/EducationalTestingService>`__ for
funding the development of GridMap.

Changelog
~~~~~~~~~

See `GitHub releases <https://github.com/EducationalTestingService/gridmap/releases>`__.
