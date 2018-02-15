import sys
import os.path
from setuptools import setup, find_packages

PACKAGE_NAME = 'nianalysis'

# Get version from module inside package
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                PACKAGE_NAME))
from package_info import __version__  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)


setup(
    name=PACKAGE_NAME,
    version=__version__,
    author='Tom G. Close',
    author_email='tom.g.close@gmail.com',
    packages=find_packages(),
    url='https://github.com/mbi-image/nianalysis',
    license='The MIT License (MIT)',
    description=(
        'A data-centric NeuroImaging analysis package based on '
        'NiPype with archive backends for XNAT and local storage'),
    long_description=open('README.rst').read(),
    install_requires=['xnat>=0.3.4',
                      'nipype>=1.0',
                      'pydicom>=0.9.9'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps."])
