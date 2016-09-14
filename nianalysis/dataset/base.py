from abc import ABCMeta
from itertools import chain
from collections import defaultdict
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Split
from logging import Logger
from nianalysis.interfaces.mrtrix import MRConvert
from nianalysis.exceptions import (
    NiAnalysisScanNameError, NiAnalysisMissingScanError)
from nianalysis.archive.base import ArchiveSource, ArchiveSink


logger = Logger('NiAnalysis')


class Dataset(object):
    """
    Base dataset class from which all derive.

    Parameters
    ----------
    project_name : str
        The name of the project. For DaRIS it is the project
        id minus the proceeding 1008.2. For XNAT it will be
        the project code. For local files it is the full path
        to the directory.
    archive : Archive
        An Archive object referring either to a DaRIS, XNAT or local file
        system project
    input_scans : Dict[str,base.Scan]
        A dict containing the a mapping between names of dataset components
        and existing scans (typically acquired from the scanner but can
        also be replacements for generated components)
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, project_id, archive, input_scans):
        self._name = name
        self._project_id = project_id
        self._input_scans = {}
        # Add each "input scan" checking to see whether the given component
        # name is valid for the dataset type
        for comp_name, scan in input_scans.iteritems():
            if comp_name not in self._components:
                raise NiAnalysisScanNameError(
                    "Input scan component name '{}' doesn't match any "
                    "components in {} datasets".format(
                        comp_name, self.__class__.__name__))
            self._input_scans[comp_name] = scan
        # Emit a warning if an acquired component has not been provided for an
        # "acquired component"
        for scan in self.acquired_components():
            if scan.name not in self._input_scans:
                logger.warning(
                    "'{}' acquired component was not specified in {} '{}' "
                    "(provided '{}'). Pipelines depending on this component "
                    "will not run".format(
                        scan.name, self.__class__.__name__, self.name,
                        "', '".join(self._input_scans)))
        # TODO: Check that every session has the acquired scans
        self._archive = archive

    def __repr__(self):
        """String representation of the dataset"""
        return "{}(name='{}')".format(self.__class__.__name__, self.name)

    def scan(self, name):
        """
        Returns either the scan that has been passed to the dataset __init__
        matching the component name provided or the processed scan that is to
        be generated using the pipeline associated with the generated component

        Parameters
        ----------
        scan : Str
            Name of the component to the find the corresponding acquired scan
            or processed scan to be generated
        """
        try:
            scan = self._input_scans[name]
        except KeyError:
            try:
                scan = self._components[name].apply_prefix(self.name + '_')
            except KeyError:
                raise NiAnalysisScanNameError(
                    "'{}' is not a recognised component name for {} datasets."
                    .format(name, self.__class__.__name__))
            if not scan.processed:
                raise NiAnalysisMissingScanError(
                    "Acquired (i.e. non-generated) scan '{}' is required for "
                    "requested pipelines but was not supplied when the dataset"
                    "was initiated.".format(name))
        return scan

    @property
    def project_id(self):
        """Accessor for the project id"""
        return self._project_id

    @property
    def name(self):
        """Accessor for the unique dataset name"""
        return self._name

    @property
    def archive(self):
        """Accessor for the archive member (e.g. Daris, XNAT, MyTardis)"""
        return self._archive

    def _create_pipeline(self, *args, **kwargs):
        """
        Creates a Pipeline object, passing the dataset (self) as the first
        argument
        """
        return Pipeline(self, *args, **kwargs)

    @classmethod
    def component(cls, name):
        """
        Return the component, i.e. the template of the scan expected to be
        supplied or generated corresponding to the component name.

        Parameters
        ----------
        name : Str
            Name of the component to return
        """
        return cls._components[name]

    @classmethod
    def component_names(cls):
        """Lists the names of all components defined in the dataset"""
        return cls._components.iterkeys()

    @classmethod
    def components(cls):
        """Lists all components defined in the dataset class"""
        return cls._components.itervalues()

    @classmethod
    def acquired_components(cls):
        """
        Lists all components defined in the dataset class that are provided as
        inputs to the dataset
        """
        return (c for c in cls.components() if not c.processed)

    @classmethod
    def generated_components(cls):
        """
        Lists all components defined in the dataset class that are typically
        generated from other components (but can be overridden in input scans)
        """
        return (c for c in cls.components() if c.processed)

    @classmethod
    def generated_component_names(cls):
        """Lists the names of generated components defined in the dataset"""
        return (c.name for c in cls.generated_components())

    @classmethod
    def acquired_component_names(cls):
        """Lists the names of acquired components defined in the dataset"""
        return (c.name for c in cls.acquired_components())


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Dataset objects.

    Parameters
    ----------
    name : str
        The name of the pipeline
    dataset : Dataset
        The dataset from which the pipeline was created
    workflow : nipype.Workflow
        The NiPype workflow to run
    inputs : List[BaseFile]
        The list of input files required for the pipeline
        un/processed files, and the options used to generate them for
        unprocessed files
    outputs : List[ProcessedFile]
        The list of outputs (hard-coded names for un/processed scans/files)
    options : Dict[str, *]
        Options that effect the output of the pipeline
    citations : List[Citation]
        List of citations that describe the workflow and should be cited in
        publications
    requirements : List[Requirement]
        List of external package requirements (e.g. FSL, MRtrix) required
        by the pipeline
    approx_runtime : float
        Approximate run time in minutes. Should be conservative so that
        it can be used to set time limits on HPC schedulers
    min_nthreads : int
        The minimum number of threads the pipeline requires to run
    max_nthreads : int
        The maximum number of threads the pipeline can use effectively.
        Use None if there is no effective limit
    """

    def __init__(self, dataset, name, inputs, outputs, description,
                 options, citations, requirements, approx_runtime,
                 min_nthreads=1, max_nthreads=1):
        # Check for unrecognised inputs/outputs
        unrecog_inputs = set(n for n in inputs
                             if n not in dataset.component_names())
        assert not unrecog_inputs, (
            "'{}' are not valid inputs names for {} dataset ('{}')"
            .format("', '".join(unrecog_inputs), dataset.__class__.__name__,
                    "', '".join(dataset.component_names())))
        self._inputs = inputs
        unrecog_outputs = set(n for n in outputs
                              if n not in dataset.generated_component_names())
        assert not unrecog_outputs, (
            "'{}' are not valid output names for {} dataset ('{}')"
            .format("', '".join(unrecog_outputs), dataset.__class__.__name__,
                    "', '".join(dataset.generated_component_names())))
        self._name = name
        self._dataset = dataset
        self._workflow = pe.Workflow(name=name)
        self._outputs = defaultdict(list)
        for output in outputs:
            mult = self._dataset.component(output).multiplicity
            self._outputs[mult].append(output)
        self._outputnodes = {}
        for mult in self._outputs:
            self._outputnodes[mult] = pe.Node(
                IdentityInterface(fields=self._outputs[mult]),
                name="{}_{}_outputnode".format(name, mult))
        # Create sets of unconnected inputs/outputs
        self._unconnected_inputs = set(inputs)
        self._unconnected_outputs = set(outputs)
        assert len(inputs) == len(self._unconnected_inputs), (
            "Duplicate inputs found in '{}'".format("', '".join(inputs)))
        assert len(outputs) == len(self._unconnected_outputs), (
            "Duplicate outputs found in '{}'".format("', '".join(outputs)))
        self._inputnode = pe.Node(IdentityInterface(fields=inputs),
                                  name="{}_inputnode".format(name))
        self._citations = citations
        self._options = options
        self._description = description
        self._requirements = requirements
        self._approx_runtime = approx_runtime
        self._min_nthreads = min_nthreads
        self._max_nthreads = max_nthreads

    def __repr__(self):
        return "Pipeline(name='{}')".format(self.name)

    def __eq__(self, other):
        # NB: Workflows should be the same for pipelines of the same name so
        #     may not need to be checked.
        return (
            self._name == other._name and
            self._dataset == other._dataset and
            self._workflow == other._workflow and
            self._inputs == other._inputs and
            self._outputs == other._outputs and
            self._options == other._options and
            self._citations == other._citations and
            self._requirements == other._requirements and
            self._approx_runtime == other._approx_runtime and
            self._min_nthreads == other._min_nthreads and
            self._max_nthreads == other._max_nthreads)

    def __ne__(self, other):
        return not (self == other)

    def run(self, subject_ids=None, session_ids=None, work_dir=None,
            reprocess=False, project=None):
        """
        Gets a data source and data sink from the archive for the requested
        sessions, connects them to the pipeline's NiPyPE workflow and runs
        the pipeline

        Parameters
        ----------
        subject_ids : List[str]
            The subset of subject IDs to process. If None all available will be
            reprocessed
        session_ids: List[str]
            The subset of session IDs to process. If None all available will be
            reprocessed
        work_dir : str
            A directory in which to run the nipype workflows
        reprocess: True|False|'all'
            A flag which determines whether to rerun the processing for this
            step. If set to 'all' then pre-requisite pipelines will also be
            reprocessed.
        project: Project
            Project info loaded from archive. It is typically only passed to
            runs of prerequisite pipelines to avoid having to requery the
            archive. If None, the project info is loaded from the dataset
            archive.
        """
        # Check all inputs and outputs are connected
        self.assert_connected()
        multiplicities = [self._dataset.component(o).multiplicity
                          for o in self.outputs]
        # Check the outputs of the pipeline to see which has the broadest
        # scope (to determine whether the pipeline needs to be rerun and for
        # which sessions/subjects
        session_outputs = [o for o, m in zip(self.outputs, multiplicities)
                           if m == 'per_session']
        subject_outputs = [o for o, m in zip(self.outputs, multiplicities)
                           if m == 'per_subject']
        project_outputs = [o for o, m in zip(self.outputs, multiplicities)
                           if m == 'per_project']
        # Get list of available subjects and their associated sessions/scans
        # from the archive
        if project is None:
            project = self._dataset.archive.project(
                self._dataset._project_id, subject_ids=subject_ids,
                session_ids=session_ids)
        # If the pipeline can be run independently for each session check
        # to see the sessions which have already been completed and omit
        # them from the sessions to be processed

        # Get all requested sessions that are missing at least one of
        # the output files
        if reprocess or not all(o in project.scans for o in project_outputs):
            subjects_to_process = list(project.subjects)
        else:
            sessions_to_process = list(chain(*[
                (sess for sess in subj.sessions
                 if not all(o in sess.scans for o in session_outputs))
                for subj in project.subjects]))
            subjects_to_process = set(
                sess.subject for sess in sessions_to_process)
            subjects_to_process |= set(
                subj for subj in project.subjects
                if not all(o in subj.scans for o in subject_outputs))
            if not subjects_to_process and not sessions_to_process:
                logger.info(
                    "Pipeline '{}' wasn't run as all requested sessions "
                    "were present")
                return  # No sessions need to be rerun
        # Run prerequisite pipelines and save their results into the archive
        for prereq in self.prerequisities:
            # NB: Even if reprocess==True, the prerequisite pipelines are not
            #     re-processed, they are only reprocessed if reprocess == 'all'
            prereq.run([s.id for s in subjects_to_process], session_ids,
                       work_dir, (reprocess if reprocess == 'all' else False),
                       project=project)
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow = pe.Workflow(name=self.name, base_dir=work_dir)
        complete_workflow.add_nodes([self._workflow])
        # Generate an input node for the sessions iterable
        sessions = pe.Node(IdentityInterface(['subject_id', 'session_id']),
                           name='sessions')
        complete_workflow.add_nodes([sessions])
        if subject_outputs or project_outputs:
            # If subject or project outputs iterate through subjects and
            # sessions independently (like nested 'for' loops)
            most_sessions = []
            if session_ids is None:
                for subject in subjects_to_process:
                    subject_session_ids = set(s.id for s in subject.sessions)
                    if len(subject_session_ids) > len(most_sessions):
                        most_sessions = subject_session_ids
                    if session_ids is None:
                        session_ids = subject_session_ids
                    else:
                        session_ids &= subject_session_ids
            if len(session_ids) < len(most_sessions):
                logger.warning(
                    "Not all sessions will be processed for some subjects as "
                    "there are an inconsistent number of sessions between "
                    "subjects.\n"
                    "Intersection of sessions: '{}'\n"
                    "Subject with most sessions: '{}'".format(
                        "', '".join(session_ids), "', '".join(most_sessions)))
            subjects = pe.Node(IdentityInterface(['subject_id']),
                               name='subjects')
            complete_workflow.add_nodes([subjects])
            subjects.iterables = ('subject_id',
                                  tuple(s.id for s in subjects_to_process))
            sessions.iterables = ('session_id', tuple(session_ids))
            complete_workflow.connect(subjects, 'subject_id',
                                      sessions, 'subject_id')
        else:
            # If only session outputs loop through all subject/session pairs
            # so that complete sessions can be skipped for subjects they are
            # required for.
            preinputnode = pe.Node(IdentityInterface(['id_pair']),
                                   name='preinputnode')
            preinputnode.iterables = (
                'id_pair',
                [[s.subject.id, s.id] for s in sessions_to_process])
            splitnode = pe.Node(Split(), name='splitnode')
            splitnode.inputs.splits = [1, 1]
            splitnode.inputs.squeeze = True
            complete_workflow.add_nodes((preinputnode, splitnode))
            complete_workflow.connect(preinputnode, 'id_pair',
                                      splitnode, 'inlist')
            complete_workflow.connect(splitnode, 'out1',
                                      sessions, 'subject_id')
            complete_workflow.connect(splitnode, 'out2',
                                      sessions, 'session_id')
        # Create source and sinks from the archive
        source = self._dataset.archive.source(
            self._dataset.project_id,
            (self._dataset.scan(i) for i in self.inputs))
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(sessions, 'subject_id',
                                  source, 'subject_id')
        complete_workflow.connect(sessions, 'session_id',
                                  source, 'session_id')
        for inpt in self.inputs:
            # Get the scan corresponding to the pipeline's input
            scan = self._dataset.scan(inpt)
            # Get the component (scan template) corresponding to the pipeline's
            # input
            comp = self._dataset.component(inpt)
            # If the scan is not in the required format for the dataset
            # user MRConvert to convert it
            if scan.format != comp.format:
                conversion = pe.Node(MRConvert(),
                                     name=(comp.name + '_input_conversion'))
                conversion.inputs.out_ext = comp.format.extension
                conversion.inputs.quiet = True
                complete_workflow.connect(source, scan.filename,
                                          conversion, 'in_file')
                scan_source = conversion
                scan_name = 'out_file'
            else:
                scan_source = source
                scan_name = scan.name + ArchiveSource.OUTPUT_SUFFIX
            # Connect the scan to the pipeline input
            complete_workflow.connect(scan_source, scan_name,
                                      self.inputnode, inpt)
        # Connect all outputs to the archive sink
        for mult, outputs in self._outputs.iteritems():
            # Create a new sink for each multiplicity level (i.e 'per_session',
            # 'per_subject' or 'per_project')
            sink = self._dataset.archive.sink(
                self._dataset._project_id,
                (self._dataset.scan(i) for i in outputs), mult)
            sink.inputs.description = self.description
            sink.inputs.name = self._dataset.name
            if mult != 'per_project':
                complete_workflow.connect(sessions, 'subject_id',
                                          sink, 'subject_id')
                if mult == 'per_session':
                    complete_workflow.connect(sessions, 'session_id',
                                              sink, 'session_id')
            for output in outputs:
                scan = self._dataset.scan(output)
                if scan.processed:  # Skip scans which are already input scans
                    complete_workflow.connect(
                        self._outputnodes[mult], scan.name,
                        sink, scan.name + ArchiveSink.INPUT_SUFFIX)
        # Run the workflow
        complete_workflow.run()

    @property
    def prerequisities(self):
        """
        Recursively append prerequisite pipelines along with their
        prerequisites onto the list of pipelines if they are not already
        present
        """
        # Loop through the inputs to the pipeline and add the instancemethods
        # for the pipelines to generate each of the processed inputs
        pipelines = set()
        for input in self.inputs:  # @ReservedAssignment
            comp = self._dataset.component(input)
            if comp.processed:
                pipelines.add(comp.pipeline)
        # Call pipeline instancemethods to dataset with provided options
        return (p(self._dataset, **self.options) for p in pipelines)

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

    def connect_input(self, inpt, node, node_input):  # @ReservedAssignment
        assert inpt in self._inputs, (
            "'{}' is not a valid input for '{}' pipeline ('{}')"
            .format(input, self.name, "', '".join(self._inputs)))
        self._workflow.connect(self._inputnode, inpt, node, node_input)
        if inpt in self._unconnected_inputs:
            self._unconnected_inputs.remove(inpt)

    def connect_output(self, output, node, node_output):
        assert output in chain(*self._outputs.values()), (
            "'{}' is not a valid output for '{}' pipeline ('{}')"
            .format(output, self.name,
                    "', '".join(chain(*self._outputs.values()))))
        assert output in self._unconnected_outputs, (
            "'{}' output has been connected already")
        outputnode = self._outputnodes[
            self._dataset.component(output).multiplicity]
        self._workflow.connect(node, node_output, outputnode, output)
        self._unconnected_outputs.remove(output)

    @property
    def name(self):
        return self._name

    @property
    def workflow(self):
        return self._workflow

    @property
    def inputs(self):
        return iter(self._inputs)

    @property
    def outputs(self):
        return chain(*self._outputs.values())

    @property
    def options(self):
        return self._options

    @property
    def description(self):
        return self._description

    @property
    def inputnode(self):
        return self._inputnode

    @property
    def session_outputnode(self):
        return self._session_outputnode

    @property
    def subject_outputnode(self):
        return self._subject_outputnode

    @property
    def project_outputnode(self):
        return self._project_outputnode

    @property
    def approx_runtime(self):
        return self._approx_runtime

    @property
    def min_nthreads(self):
        return self._min_nthreads

    @property
    def max_nthreads(self):
        return self._max_nthreads

    @property
    def suffix(self):
        """
        A suffixed appended to output filenames when they are archived to
        identify the options used to generate them
        """
        return '__'.join('{}_{}'.format(k, v)
                         for k, v in self.options.iteritems())

    def assert_connected(self):
        """
        Check for unconnected inputs and outputs after pipeline construction
        """
        assert not self._unconnected_inputs, (
            "'{}' input{} not connected".format(
                "', '".join(self._unconnected_inputs),
                ('s are' if len(self._unconnected_inputs) > 1 else ' is')))
        assert not self._unconnected_outputs, (
            "'{}' output{} not connected".format(
                "', '".join(self._unconnected_outputs),
                ('s are' if len(self._unconnected_outputs) > 1 else ' is')))


def _create_component_dict(*comps, **kwargs):
    dct = {}
    for comp in comps:
        if comp.name in dct:
            assert False, ("Multiple values for '{}' found in component list"
                           .format(comp.name))
        dct[comp.name] = comp
    if 'inherit_from' in kwargs:
        combined = _create_component_dict(*kwargs['inherit_from'])
        combined.update(dct)
        dct = combined
    return dct
