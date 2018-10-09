Arcana
======

.. image:: https://travis-ci.org/MonashBI/arcana.svg?branch=master
  :target: https://travis-ci.org/MonashBI/arcana
.. image:: https://codecov.io/gh/MonashBI/arcana/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/MonashBI/arcana
.. image:: https://img.shields.io/pypi/pyversions/arcana.svg
  :target: https://pypi.python.org/pypi/arcana/
  :alt: Supported Python versions
.. image:: https://img.shields.io/pypi/v/arcana.svg
  :target: https://pypi.python.org/pypi/arcana/
  :alt: Latest Version    
.. image:: https://readthedocs.org/projects/arcana/badge/?version=latest
  :target: http://arcana.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


Abstraction of Repository-Centric ANAlysis (Arcana) is Python framework
for "repository-centric" analyses of study groups (e.g. NeuroImaging studies)

Arcana interacts closely with a repository, storing intermediate
outputs, along with the parameters used to derive them, for reuse by
subsequent analyses. Repositories can either be XNAT repositories or
(http://xnat.org) local directories organised by subject and visit,
and a BIDS module (http://bids.neuroimaging.io/) is planned as future
work. 

Analysis workflows are constructed and executed using the NiPype
package, and can either be run locally or submitted to high HPC
facilities using NiPype’s execution plugins. For a requested analysis
output, Arcana determines the required processing steps by querying
the repository to check for missing intermediate outputs before
constructing the workflow graph. When running in an environment
with `the modules package <http://modules.sourceforge.net>`_ installed,
Arcana manages the loading and unloading of software modules per
pipeline node.


User/Developer Guide
--------------------

.. toctree::
    :maxdepth: 2 

    installation
    design
    example
    api_design
    api_application
