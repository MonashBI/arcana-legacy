import json


class PipelineRecord(object):
    """
    A representation of the information required to describe the provenance of
    study derivatives.

    Parameters
    ----------
    study_parameters : list[Parameter]
        List of study parameters that are used to configure the pipeline
    interface_parameters : dict[str, dict[str, *]]
        Fixed input parameters for each node of the pipeline (i.e. not pipeline
        inputs)
    requirement_versions : dict[str, list[Version]]
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
    inputs : dict[str, str | list[str] | list[list[str]]] | None
        Checksums of all pipeline inputs used by the pipeline. For inputs
        of matching frequency to the output derivative associated with the
        provenance object, the values of the dictionary will be single
        checksums. If the output is of lower frequency they will be lists of
        checksums or in the case of 'per_session' inputs to 'per_study'
        outputs, lists of lists of checksum. They can be omitted from the
        initialisation and inserted on write of each sink node during the
        workflow run to allow the same pipeline object to be reused between
        different sink node instances.
    outputs : dict[str, str] | None
        Checksums of all pipeline outputs. They can be omitted from the
        initialisation and inserted on write of each sink node during the
        workflow run to allow the same pipeline object to be reused between
        different sink node instances.
    """

    def __init__(self, pipeline_name, study_parameters, interface_parameters,
                 requirement_versions, arcana_version, nipype_version,
                 workflow_graph, subject_ids, visit_ids):
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
        return self.match(other) and self.match_versions(other)

    def matches(self, other, ignore_versions=False):
        """
        Compares information stored within provenance objects with the
        exception of version information to see if they match.

        Parameters
        ----------
        other : Provenance
            The provenance object to compare against
        """
        if ignore_versions:
            versions_match = True
        else:
            versions_match = (
                self._requirement_versions == other._requirement_versions and
                self._arcana_version == other._arcana_version and
                self._nipype_version == other._nipype_version)
        return (
            versions_match and
            self._pipeline_name == other._pipeline_name and
            self._study_parameters == other._study_parameters and
            self._interface_parameters == other._interface_parameters and
            self._workflow_graph == other._workflow_graph and
            self._subject_ids == other._subject_ids and
            self._visit_ids == other._visit_ids)

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

    def record(self, inputs, outputs):
        return Record(self, inputs, outputs)


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
    """

    def __init__(self, pipeline_record, inputs, outputs):
        self._pipeline_record = pipeline_record
        self._inputs = inputs
        self._outputs = outputs

    @property
    def pipeline_record(self):
        return self._pipeline_record

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def matches(self, other, ignore_versions=False):
        return (
            self._pipeline_record.matches(
                other, ignore_versions=ignore_versions) and
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
            'pipeline_name': self._pipeline_name,
            'study_parameters': self._study_parameters,
            'interface_parameters': self._interface_parameters,
            'requirement_versions': self._requirement_versions,
            'arcana_version': self._arcana_version,
            'nipype_version': self._nipype_version,
            'workflow_graph': self._workflow_graph,
            'subject_ids': self._subject_ids,
            'visit_ids': self._visit_ids,
            'inputs': self.inputs,
            'outputs': self.outputs}
        with open(path, 'w') as f:
            json.dump(dct, f)

    @classmethod
    def load(cls, path):
        """
        Loads a saved provenance object from a JSON file

        Parameters
        ----------
        path : str
            Path to the provenance file

        Returns
        -------
        prov : Provenance
            The loaded provenance object
        """
        with open(path) as f:
            dct = json.load(f)
        pipeline_record = PipelineRecord(
            pipeline_name=dct['pipeline_name'],
            study_parameters=dct['study_parameters'],
            interface_parameters=dct['interface_parameters'],
            requirement_versions=dct['requirement_versions'],
            arcana_version=dct['arcana_version'],
            nipype_version=dct['nipype_version'],
            workflow_graph=dct['workflow_graph'],
            subject_ids=dct['subject_ids'],
            visit_ids=dct['visit_ids'])
        return Record(pipeline_record, inputs=dct['inputs'],
                      outputs=dct['outputs'])
