# -*- coding: utf-8 -*-

# Written (W) 2008-2012 Christian Widmer
# Written (W) 2008-2010 Cheng Soon Ong
# Written (W) 2012-2013 Daniel Blanchard, dblanchard@ets.org
# Copyright (C) 2008-2012 Max-Planck-Society, 2012-2013 ETS

# This file is part of GridMap.

# GridMap is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# GridMap is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with GridMap.  If not, see <http://www.gnu.org/licenses/>.
from setuptools import setup

# To get around the fact that you can't import stuff from packages in setup.py
exec(compile(open('gridmap/version.py').read(), 'gridmap/version.py', 'exec'))
# (we use the above instead of execfile for Python 3.x compatibility)

def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='gridmap',
      version=__version__,
      description=('Easily map Python functions onto a cluster using a ' +
                   'DRMAA-compatible grid engine like Sun Grid Engine (SGE).'),
      long_description=readme(),
      keywords='drmaa sge cluster distributed parallel',
      url='http://github.com/EducationalTestingService/gridmap',
      author='Daniel Blanchard',
      author_email='dblanchard@ets.org',
      license='GPL',
      packages=['gridmap'],
      install_requires=['drmaa', 'pyzmq'],
      classifiers=['Intended Audience :: Science/Research',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
                   'Programming Language :: Python',
                   'Topic :: Software Development',
                   'Topic :: Scientific/Engineering',
                   'Operating System :: POSIX',
                   'Operating System :: Unix',
                   'Operating System :: MacOS',
                   'Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.3'],
      zip_safe=False)
