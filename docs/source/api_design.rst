API - Design
============

The Arcana public API is based around the Study class, which should
be sub-classed to implement workflows.


Base Study Classes
------------------

.. autoclass:: arcana.study.Study
    :members: data, new_pipeline, pipeline, inputs, branch, parameter, unhandled_branch, data_spec, parameter_spec

.. autoclass:: arcana.study.MultiStudy
    :members: sub_study, translate

.. autoclass:: arcana.study.ParameterSpec

.. autoclass:: arcana.study.SubStudySpec


Meta-classes
------------

.. autoclass:: arcana.study.StudyMetaClass

.. autoclass:: arcana.study.MultiStudyMetaClass


Data Specs
----------

.. autoclass:: arcana.data.FilesetSpec

.. autoclass:: arcana.data.FieldSpec

.. autoclass:: arcana.data.FileFormat


Pipeline
--------

.. autoclass:: arcana.pipeline.Pipeline
    :members: add, provided, parameter, connect, connect_input, connect_output 


Requirements
------------

.. autoclass:: arcana.environment.CliRequirement

.. autoclass:: arcana.environment.MatlabPackageRequirement

.. autoclass:: arcana.environment.PythonPackageRequirement


Misc
----

.. autoclass:: arcana.citation.Citation