Design
======

Arcana is designed with an object-oriented philosophy, with
the acquired and derived data sets along with the analysis pipelines
used to derive the derived data sets encapsulated within "Study" classes.

The Arcana package itself only provides the abstract *Study* and *MultiStudy*
base classes, which are designed to be sub-classed to provide specialised
classes representing the analysis that can be performed on specific types of
data (e.g. BoldStudy, PetStudy). These specific classes can then be sub-classed
further into classes that are specific to the a particular study, and integrate
the complete workflow from preprocessing to statistic analysis.
