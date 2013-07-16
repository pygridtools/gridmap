from setuptools import setup

from gridmap import __version__

def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='gridmap',
      version=__version__,
      description='Easily map Python functions onto a cluster using a DRMAA-compatible grid engine like Sun Grid Engine (SGE).',
      long_description=readme(),
      keywords='drmaa sge cluster distributed parallel',
      url='http://github.com/EducationalTestingService/gridmap',
      author='Daniel Blanchard',
      author_email='dblanchard@ets.org',
      license='GPL',
      packages=['gridmap'],
      install_requires=['drmaa', 'redis', 'hiredis'],
      zip_safe=False)
