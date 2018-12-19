API - Design
============

The Arcana public API is based around the Study class, which should
be sub-classed to implement workflows.


Study
-----

.. autoclass:: arcana.study.Study
    :members: data, pre_option, new_pipeline

.. autoclass:: arcana.study.MultiStudy

Meta-classes
------------

.. autoclass:: arcana.study.StudyMetaClass

.. autoclass:: arcana.study.MultiStudyMetaClass


Specs
-----

.. autoclass:: arcana.data.FilesetSpec

.. autoclass:: arcana.data.FieldSpec

.. autoclass:: arcana.study.multi.SubStudySpec

.. autoclass:: arcana.option.OptionSpec


Pipeline
--------

.. autoclass:: arcana.pipeline.Pipeline
    :members: add, provided, parameter, connect, connect_input, connect_output 


Other
-----

.. autoclass:: arcana.requirement.Requirement

.. autoclass:: arcana.data_format.DataFormat

.. autoclass:: arcana.citation.Citation