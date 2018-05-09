Arcana
======

.. image:: https://travis-ci.org/monashbiomedicalimaging/arcana.svg?branch=master
  :target: https://travis-ci.org/monashbiomedicalimaging/arcana
.. image:: https://codecov.io/gh/monashbiomedicalimaging/arcana/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/monashbiomedicalimaging/arcana


ARchive Centric ANAlysis (ARCANA) is Python package for "archive-centric" 
analysis of study groups (e.g. NeuroImaging studies)

Arcana interacts closely with an archive, storing intermediate
outputs, along with the parameters used to derive them, for reuse by
subsequent analyses. Archives can either be XNAT repositories or
(http://xnat.org) local directories organised by subject and visit,
and a BIDS module (http://bids.neuroimaging.io/) is planned as future
work. 

Analysis workflows are constructed and executed using the NiPype
package, and can either be run locally or submitted to high HPC
facilities using NiPype’s execution plugins. For a requested analysis
output, Arcana determines the required processing steps by querying
the archive to check for missing intermediate outputs before
constructing the workflow graph. When running in an environment
with ` the modules package <http://modules.sourceforge.net>`_ installed,
Arcana manages the loading and unloading of software modules per
pipeline node.

Design
------

Arcana is designed with an object-oriented philosophy, with
the acquired and derived data sets along with the analysis pipelines
used to derive the derived data sets encapsulated within "Study" classes.

The Arcana package itself only provides the abstract *Study* and
*CombinedStudy* base classes, which are designed to be sub-classed by
more specific classes representing the analysis that can be performed
on different modalities and contrasts (e.g. PetStudy, DiffusionMriStudy,
FmriStudy). These contrast/modality classes are intended to be sub-classed and
combined into classes that are specific to the a particular PET|MRI study,
class (e.g. ASPREE Neuro), and integrate the complete workflow from preprocessing
to statistic analysis.

Installation
------------

Arcana can be installed using ``pip``::

    $ pip install git+https://github.com/monashbiomedicalimaging/arcana.git

although for most pipelines you will also need to install the relevant
tools that are called on to the the processing 



