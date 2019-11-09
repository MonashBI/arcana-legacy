API - Design
============

The Arcana public API is based around the Analysis class, which should
be sub-classed to implement workflows.


Analysis
-----

.. autoclass:: arcana.analysis.Analysis
    :members: data, new_pipeline, pipeline, provided, branch, parameter, unhandled_branch, data_spec, param_spec

.. autoclass:: arcana.analysis.MultiAnalysis
    :members: subcomp, translate

.. autoclass:: arcana.analysis.ParamSpec

.. autoclass:: arcana.analysis.SubCompSpec


Meta-classes
------------

.. autoclass:: arcana.analysis.AnalysisMetaClass

.. autoclass:: arcana.analysis.MultiAnalysisMetaClass


Data Specs
----------

.. autoclass:: arcana.data.FilesetSpec

.. autoclass:: arcana.data.FieldSpec

.. autoclass:: arcana.data.FileFormat


Pipeline
--------

.. autoclass:: arcana.pipeline.Pipeline
    :members: add, provided, connect, connect_input, connect_output 


Requirements
------------

.. autoclass:: arcana.environment.CliRequirement

.. autoclass:: arcana.environment.MatlabPackageRequirement

.. autoclass:: arcana.environment.PythonPackageRequirement


Misc
----

.. autoclass:: arcana.citation.Citation