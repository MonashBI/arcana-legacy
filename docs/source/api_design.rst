API - Design
============

The Arcana public API is based around the Study class, which should
be sub-classed to implement workflows.


Study
-----

.. autoclass:: arcana.study.Study
    :members: data, pre_option, create_pipeline

.. autoclass:: arcana.study.MultiStudy

Meta-classes
------------

.. autoclass:: arcana.study.StudyMetaClass

.. autoclass:: arcana.study.MultiStudyMetaClass


Specs
-----

.. autoclass:: arcana.dataset.DatasetSpec

.. autoclass:: arcana.dataset.FieldSpec

.. autoclass:: arcana.study.multi.SubStudySpec

.. autoclass:: arcana.option.OptionSpec


Pipeline
--------

.. autoclass:: arcana.pipeline.Pipeline
    :members: connect, connect_input, connect_output, connect_subject_id, connect_visit_id, create_node, create_map_node, create_join_node, create_join_visits_node, create_join_subjects_node, option 


Other
-----

.. autoclass:: arcana.requirement.Requirement

.. autoclass:: arcana.data_format.DataFormat

.. autoclass:: arcana.citation.Citation