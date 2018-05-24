==========
Public API
==========

The Arcana public API is based around the Study class, and classes that
are provided either to sub-class definitions or Study instantiations.

Study
-----

.. autoclass:: arcana.study.Study
    :members: data

.. autoclass:: arcana.study.MultiStudy


Archives
--------

.. autoclass:: arcana.archive.xnat.XnatArchive

.. autoclass:: arcana.archive.local.LocalArchive

Runners
-------

.. autoclass:: arcana.runner.LinearRunner

.. autoclass:: arcana.runner.MultiProcRunner

.. autoclass:: arcana.runner.SlurmRunner



Datasets and Fields
-------------------

.. autoclass:: arcana.dataset.Dataset

.. autoclass:: arcana.dataset.Field

.. autoclass:: arcana.dataset.DatasetSpec

.. autoclass:: arcana.dataset.FieldSpec

.. autoclass:: arcana.dataset.DatasetMatch

.. autoclass:: arcana.dataset.FieldMatch

Options
-------

.. autoclass:: arcana.option.Option

.. autoclass:: arcana.option.OptionSpec


Project tree
------------

.. autoclass:: arcana.archive.Project

.. autoclass:: arcana.archive.Subject

.. autoclass:: arcana.archive.Visit

.. autoclass:: arcana.archive.Session


Other
-----

.. autoclass:: arcana.requirement.Requirement

.. autoclass:: arcana.data_format.DataFormat
