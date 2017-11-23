from setuptools import setup, find_packages

# Get information about the version (polling mercurial if possible)
version = '0.1'

setup(
    name='nianalysis',
    version=version,
    author='Tom G. Close',
    author_email='tom.g.close@gmail.com',
    packages=find_packages(),
    url='https://gitlab.erc.monash.edu.au/mbi-image/NiAnalysis',
    license='The MIT License (MIT)',
    description=(
        'An data-centric NeuroImaging analysis processing package based on '
        'NiPype with archive backends for XNAT and DaRIS'),
    long_description=open('README.rst').read(),
    install_requires=['xnat>=0.3.4',
                      'nipype>=0.14.0-rc1',
                      'pydicom>=0.9.9'],
    dependency_links=[
        "git+https://github.com/nipy/nipype#egg=nipype-0.14.0-rc1"
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Healthcare Industry",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Medical Science Apps."])
