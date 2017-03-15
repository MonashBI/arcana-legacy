from setuptools import setup

# Get information about the version (polling mercurial if possible)
version = '0.1'

setup(
    name='nianalysis',
    version=version,
    author='Tom G. Close',
    author_email='tom.g.close@gmail.com',
    packages=['nianalysis'],
    url='https://gitlab.erc.monash.edu.au/mbi-image/NiAnalysis',
    license='The MIT License (MIT)',
    description=(
        'An data-centric NeuroImaging analysis processing package based on '
        'NiPype with archive backends for XNAT and DaRIS'),
    long_description=open('README.rst').read(),
    install_requires=['xnat', 'nipype'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps."])
