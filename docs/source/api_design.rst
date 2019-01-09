API - Design
============

The Arcana public API is based around the Study class, which should
be sub-classed to implement workflows.


Study
-----

.. autoclass:: arcana.study.Study
    :members: data, new_pipeline, pipeline, inputs, branch, parameter, unhandled_branch, data_spec, parameter_spec

.. autoclass:: arcana.study.MultiStudy
    :members: sub_study, translate

Meta-classes
------------

.. autoclass:: arcana.study.StudyMetaClass

.. autoclass:: arcana.study.MultiStudyMetaClass


Specs
-----

.. autoclass:: arcana.data.FilesetSpec

.. autoclass:: arcana.data.FieldSpec

.. autoclass:: arcana.study.SubStudySpec

.. autoclass:: arcana.study.ParameterSpec


Pipeline
--------

.. autoclass:: arcana.pipeline.Pipeline
    :members: add, provided, parameter, connect, connect_input, connect_output 


Requirements
------------

.. autoclass:: CliRequirement

.. autoclass:: MatlabPackageRequirement,

.. autoclass:: PythonPackageRequirement


Misc
----

.. autoclass:: arcana.citation.Citation