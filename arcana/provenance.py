import json
from arcana.utils import find_mismatch


class PipelineRecord(object):
    """
    A representation of the information required to describe the provenance of
    study derivatives.

    Parameters
    ----------
    study_name : str
        The name of the study that generated the pipeline. Used to store and
        retrieve the provenance from repositories. Note that it is not included
        in the matching logic so you can test provenance from separate studies
    pipeline_name : str
        Name of the pipeline
    study_parameters : list[Parameter]
        List of study parameters that are used to configure the pipeline
    interface_parameters : dict[str, dict[str, *]]
        Fixed input parameters for each node of the pipeline (i.e. not pipeline
        inputs)
    requirement_versions : dict[str, list[(name, ver_str, local_name, local_ver)]
        A list of software versions used by each interface in the pipeline
    arcana_version : str
        The version of Arcana
    nipype_version : str
        The version of Nipype
    workflow_graph : ?
        The graph of the pipeline workflow
    subject_ids : list[str]
        The subject IDs included in the analysis
    visit_ids : list[str]
        The visit IDs included in the analysis
    """

    def __init__(self, study_name, pipeline_name, study_parameters,
                 interface_parameters, requirement_versions, arcana_version,
                 nipype_version, workflow_graph, subject_ids, visit_ids):
        self._study_name = study_name
        self._pipeline_name = pipeline_name
        self._study_parameters = study_parameters
        self._interface_parameters = interface_parameters
        self._requirement_versions = requirement_versions
        self._arcana_version = arcana_version
        self._nipype_version = nipype_version
        self._workflow_graph = workflow_graph
        self._subject_ids = subject_ids
        self._visit_ids = visit_ids

    def __eq__(self, other):
        return self.matches(other)

    def __repr__(self):
        return "{}(pipeline='{}')".format(type(self).__name__,
                                          self.pipeline_name)

    def matches(self, other, ignore_versions=False):
        """
        Compares information stored within provenance objects with the
        exception of version information to see if they match.

        Parameters
        ----------
        other : Provenance
            The provenance object to compare against
        """
        match = (
            self._pipeline_name == other._pipeline_name and
            self._subject_ids == other._subject_ids and
            self._visit_ids == other._visit_ids and
            self._study_parameters == other._study_parameters and
            self._interface_parameters == other._interface_parameters and
            self._workflow_graph == other._workflow_graph)
        if not ignore_versions:
            match &= (
                self._requirement_versions == other._requirement_versions and
                self._arcana_version == other._arcana_version and
                self._nipype_version == other._nipype_version)
        return match

    @property
    def study_name(self):
        return self._study_name

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def study_parameters(self):
        return self._study_parameters

    @property
    def interface_parameters(self):
        return self._interface_parameters

    @property
    def requirement_versions(self):
        return self._requirement_versions

    @property
    def arcana_version(self):
        return self._arcana_version

    @property
    def nipype_version(self):
        return self._nipype_version

    @property
    def workflow_graph(self):
        return self._workflow_graph

    @property
    def subject_ids(self):
        return self._subject_ids

    @property
    def visit_ids(self):
        return self._visit_ids

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def record(self, inputs, outputs, subject_id, visit_id):
        return Record(self, inputs, outputs, subject_id, visit_id)

    def find_mismatch(self, other, indent='', ignore_versions=False):
        mismatch = ''
        if self.pipeline_name != other.pipeline_name:
            mismatch += ('\n{indent}pipeline_name: self={} v other={}'
                         .format(self.pipeline_name, other.pipeline_name,
                                 indent=indent))
        if self.study_parameters != other.study_parameters:
            mismatch += ('\n{indent}mismatching study_parameters{}'
                         .format(find_mismatch(self.study_parameters,
                                               other.study_parameters,
                                               indent=indent),
                                 indent=indent))
        if self.interface_parameters != other.interface_parameters:
            mismatch += ('\n{indent}interface_parameters{}'
                         .format(find_mismatch(self.interface_parameters,
                                               other.interface_parameters,
                                               indent=indent),
                                 indent=indent))
        if self.workflow_graph != other.workflow_graph:
            mismatch += ('\n{indent}workflow_graph{}'
                         .format(find_mismatch(self.workflow_graph,
                                               other.workflow_graph,
                                               indent=indent),
                                 indent=indent))
        if self.subject_ids != other.subject_ids:
            mismatch += ('\n{indent}subject_ids: self={} v other={}'
                         .format(self.subject_ids, other.subject_ids,
                                 indent=indent))
        if self.visit_ids != other.visit_ids:
            mismatch += ('\n{indent}visit_ids: self={} v other={}'
                         .format(self.visit_ids, other.visit_ids,
                                 indent=indent))
        if not ignore_versions:
            if self.requirement_versions != other.requirement_versions:
                mismatch += ('\n{indent}requirement_versions{}'
                             .format(find_mismatch(self.requirement_versions,
                                                   other.requirement_versions,
                                                   indent=indent),
                                     indent=indent))
            if self.arcana_version != other.arcana_version:
                mismatch += ('\n{indent}arcana_version: self={} v other={}'
                             .format(self.arcana_version,
                                     other.arcana_version))
            if self.nipype_version != other.nipype_version:
                mismatch += ('\n{indent}nipype_version: self={} v other={}'
                             .format(self.nipype_version,
                                     other.nipype_version))
        return mismatch


class Record(object):
    """
    Records the provenance information relevant to a specific session, i.e.
    the general configuration of the pipeline and file checksums|field values
    of the pipeline inputs used to derive the outputs in a given session
    (or visit, subject, study summary) as well as the checksums|values of the
    outputs (in order to detect if they have been altered outside of Arcana's
    management, e.g. manual QC|correction)

    Parameters
    ----------
    pipeline_record : PipelineRecord
        The pipeline-wide configuration provenance information
    inputs : dict[str, dict[(str, str), str | int | float | list[float] | list[int] | list[str]]]
        Checksums or field values of all inputs that have gone into to derive
        the outputs of the pipeline
    outputs : dict[str, str | int | float | list[float] | list[int] | list[str]]
        Checksums or field values of all the outputs of the pipeline
    subject_id : str | None
        The subject ID the record corresponds to. If None can be a per-visit or
        per-study summary
    visit_id : str | None
        The visit ID the record corresponds to. If None can be a per-subject or
        per-study summary
    """

    def __init__(self, pipeline_record, inputs, outputs, subject_id, visit_id):
        self._pipeline_record = pipeline_record
        self._inputs = inputs
        self._outputs = outputs
        self._subject_id = subject_id
        self._visit_id = visit_id

    @property
    def pipeline_record(self):
        return self._pipeline_record

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def from_study(self):
        return self.pipeline_record.study_name

    def matches(self, other, ignore_versions=False):
        return (
            self.pipeline_record.matches(
                other.pipeline_record, ignore_versions=ignore_versions) and
            self._inputs == other._inputs and
            self._outputs == other._outputs)

    def save(self, path):
        """
        Saves the provenance object to a JSON file, optionally including
        checksums for inputs and outputs (which are initially produced mid-
        run) to insert during the write

        Parameters
        ----------
        path : str
            Path to save the generated JSON file
        inputs : dict[str, str | list[str] | list[list[str]]] | None
            Checksums of all pipeline inputs used by the pipeline. For inputs
            of matching frequency to the output derivative associated with the
            provenance object, the values of the dictionary will be single
            checksums. If the output is of lower frequency they will be lists
            of checksums or in the case of 'per_session' inputs to 'per_study'
            outputs, lists of lists of checksum. They need to be provided here
            if the provenance object was initialised without checksums
        outputs : dict[str, str] | None
            Checksums of all pipeline outputs. They need to be provided here
            if the provenance object was initialised without checksums
        """
        dct = {
            'pipeline_name': self.pipeline_record.pipeline_name,
            'study_parameters': self.pipeline_record.study_parameters,
            'interface_parameters': self.pipeline_record.interface_parameters,
            'requirement_versions': self.pipeline_record.requirement_versions,
            'arcana_version': self.pipeline_record.arcana_version,
            'nipype_version': self.pipeline_record.nipype_version,
            'workflow_graph': self.pipeline_record.workflow_graph,
            'subject_ids': self.pipeline_record.subject_ids,
            'visit_ids': self.pipeline_record.visit_ids,
            'inputs': self.inputs,
            'outputs': self.outputs}
        with open(path, 'w') as f:
            json.dump(dct, f)

    @classmethod
    def load(cls, path, study_name, subject_id, visit_id):
        """
        Loads a saved provenance object from a JSON file

        Parameters
        ----------
        path : str
            Path to the provenance file
        study_name : str
            Name of the study the derivatives were created for
        subject_id : str | None
            The subject ID of the provenance record
        visit_id : str | None
            The visit ID of the provenance record

        Returns
        -------
        record : Record
            The loaded provenance record
        """
        with open(path) as f:
            dct = json.load(f)
        pipeline_record = PipelineRecord(
            study_name=study_name,
            pipeline_name=dct['pipeline_name'],
            study_parameters=dct['study_parameters'],
            interface_parameters=dct['interface_parameters'],
            requirement_versions=dct['requirement_versions'],
            arcana_version=dct['arcana_version'],
            nipype_version=dct['nipype_version'],
            workflow_graph=dct['workflow_graph'],
            subject_ids=dct['subject_ids'],
            visit_ids=dct['visit_ids'])
        return Record(pipeline_record, dct['inputs'], dct['outputs'],
                      subject_id, visit_id)

    def find_mismatch(self, other, indent='', **kwargs):
        mismatch = ''
        sub_indent = indent + '  '
        if self.pipeline_record != other.pipeline_record:
            mismatch += self.pipeline_record.find_mismatch(
                other.pipeline_record, indent=indent, **kwargs)
        if self.inputs != other.inputs:
            mismatch += ('\n{indent}inputs{}'
                         .format(find_mismatch(self.inputs,
                                               other.inputs,
                                               indent=sub_indent),
                                 indent=indent))
        if self.outputs != other.outputs:
            mismatch += ('\n{indent}outputs: self={} v other={}'
                         .format(self.outputs, other.outputs,
                                 indent=indent))
        return mismatch
