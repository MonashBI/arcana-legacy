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


Mastering the "`arcana <https://en.wiktionary.org/wiki/arcana>`_ of scientific
analysis workflows", the obscure knowledge required to apply an appropriate
sequence of community-developed software tools with appropriate
parameterisations in order to analyse a given dataset, can be a time consuming
and challenging process. *Abstraction of Repository-Centric ANAlysis (Arcana)*
provides a framework for researchers to collaboratively implement analyses in a
way that can be reused and extended across wide range of studies and computing
environments. Given a large enough pool of contributors, such code-bases should
mature and become valuable repositories for the arcana of the scientific field.

Central to Arcana's architecture is the implementation of analyses within
subclasses of :class:`arcana.analysis.Analysis`. Arcana interacts closely with a
repository, storing intermediate outputs, along with the parameters used to
derive them, for reuse by subsequent analyses. Repositories can either be XNAT
repositories or (http://xnat.org) local directories organised by subject and
visit, and a BIDS module (http://bids.neuroimaging.io/) is planned as future
work.

Behind the scenes, analysis workflows are constructed and executed using
`NiPype <http://nipype.readthedocs.io>`_, and can either be run locally or
submitted to high HPC facilities using NiPype’s execution plugins. For a
requested analysis output, Arcana determines the required processing steps by
querying the repository to check for missing intermediate outputs before
constructing the workflow graph. When running in an environment with `the
modules package <http://modules.sourceforge.net>`_ installed, Arcana manages the
loading and unloading of software modules per pipeline node.


User/Developer Guide
--------------------

.. toctree::
    :maxdepth: 2

    installation
    design
    example
    api_design
    api_application
