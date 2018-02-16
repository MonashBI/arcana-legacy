NiAnalysis
==========

.. image:: https://travis-ci.org/mbi-image/nianalysis.svg?branch=master
    :target: https://travis-ci.org/mbi-image/nianalysis
.. image:: https://codecov.io/gh/mbi-image/nianalysis/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/mbi-image/nianalysis

NeuroImaging Analysis (NiAnalysis) is an archive-centric NeuroImaging
analysis package.

NiAnalysis interacts closely with an archive, storing intermediate
outputs, along with the parameters used to derive them, for reuse by
subsequent analyses. Archives can either be XNAT repositories or
(http://xnat.org) local directories organised by subject and visit,
and a BIDS module (http://bids.neuroimaging.io/) is planned as future
work. 

Analysis workflows are constructed and executed using the NiPype
package, and can either be run locally or submitted to high HPC
facilities using NiPype’s execution plugins. For a requested analysis
output, NiAnalysis determines the required processing steps by querying
the archive to check for missing intermediate outputs before
constructing the workflow graph. When running on HPC infrastructure
with environment modules installed, NiAnalysis manages the loading and
unloading of software modules per pipeline node.

Design
------

NiAnalysis is designed with an object-oriented philosophy, with
the acquired and derived data sets along with the analysis pipelines
used to derive the derived data sets encapsulated within "Study" classes.

The NiAnalysis package itself only provides the abstract study base
class, which is designed to be derived by more specific Study classes
representing the analysis that can be performed on different modalities
and contrasts (e.g. PetStudy, DiffusionMriStudy, FmriStudy). These
contrast/modality classes are intended to be derived in turn can to
for Study classes that are specific to the a particular PET|MRI study,
providing a way to integrate the complete analysis from preprocessing
to statistics. 

Study classes for multi-modality/contrast studies can be constructed
using the "CombinedStudy" class, which provides a way to combine
multiple Study classes into a single combined class. 

Installation
------------

NiAnalysis itself can be installed using ``pip``::

    $ pip install git+https://github.com/mbi-image/nianalysis.git

although for most pipelines you will also need to install the relevant
neuro-imaging tools that are called on to the the processing (e.g.
FSL, SPM/Matlab, AFNI, MRtrix, etc...).

For automated file format conversion between common neuroimaging
formats (e.g. DICOM, NIfTI, MRtrix) the MRtrix (http://mrtrix.org)
and/or Dicom2niix (http://github.com/rordenlab/dcm2niix) should be
installed. Please see their documentation for instructions.



