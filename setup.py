import sys
import os.path
from setuptools import setup, find_packages

PACKAGE_NAME = 'arcana'

# Get version from module inside package
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                PACKAGE_NAME))
from version_ import __version__  # @UnresolvedImport @IgnorePep8
sys.path.pop(0)


setup(
    name=PACKAGE_NAME,
    version=__version__,
    author='Tom G. Close',
    author_email='tom.g.close@gmail.com',
    packages=find_packages(),
    url='https://github.com/monashbiomedicalimaging/arcana',
    license='The Apache Software Licence 2.0',
    description=(
        'Archive-centric analysis workflow architecture based on NiPype'),
    long_description=open('README.rst').read(),
    install_requires=['xnat>=0.3.7',
                      'nipype==1.0',
                      'pydicom>=1.0.2',
                      'networkx==1.9',
                      'fasteners>=0.7.0'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps."],
    keywords='archive analysis')
