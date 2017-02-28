from itertools import chain
from collections import defaultdict
from copy import copy
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Split
from logging import getLogger
from nianalysis.exceptions import (
    NiAnalysisDatasetNameError, NiAnalysisError, NiAnalysisMissingDatasetError)
from nianalysis.data_formats import get_converter_node
from nianalysis.interfaces.utils import InputSessions, OutputSummary
from nianalysis.utils import INPUT_SUFFIX, OUTPUT_SUFFIX


logger = getLogger('NIAnalysis')


class Pipeline(object):
    """
    Basically a wrapper around a NiPype workflow to keep track of the inputs
    and outputs a little better and provide some convenience functions related
    to the Study objects.

    Parameters
    ----------
    name : str
        The name of the pipeline
    study : Study
        The study from which the pipeline was created
    inputs : List[BaseFile]
        The list of input datasets required for the pipeline
        un/processed datasets, and the options used to generate them for
        unprocessed datasets
    outputs : List[ProcessedFile]
        The list of outputs (hard-coded names for un/processed datasets)
    default_options : Dict[str, *]
        Default options that are used to construct the pipeline. They can
        be overriden by values provided to they 'options' keyword arg
    citations : List[Citation]
        List of citations that describe the workflow and should be cited in
        publications
    requirements : List[Requirement]
        List of external package requirements (e.g. FSL, MRtrix) required
        by the pipeline
    version : int
        A version number for the pipeline to be incremented whenever the output
        of the pipeline
    approx_runtime : float
        Approximate run time in minutes. Should be conservative so that
        it can be used to set time limits on HPC schedulers
    min_nthreads : int
        The minimum number of threads the pipeline requires to run
    max_nthreads : int
        The maximum number of threads the pipeline can use effectively.
        Use None if there is no effective limit
    options : Dict[str, *]
        Options that effect the output of the pipeline that override the
        default options. Extra options that are not in the default_options
        dictionary are ignored
    """

    def __init__(self, study, name, inputs, outputs, description,
                 default_options, citations, requirements, approx_runtime,
                 version, min_nthreads=1, max_nthreads=1, options={}):
        self._name = name
        self._study = study
        self._workflow = pe.Workflow(name=name)
        self._version = int(version)
        # Set up inputs
        self._check_spec_names(inputs, 'input')
        self._inputs = inputs
        self._inputnode = pe.Node(IdentityInterface(
            fields=list(self.input_names)),
            name="{}_inputnode".format(name))
        # Set up outputs
        self._check_spec_names(outputs, 'output')
        self._outputs = defaultdict(list)
        for output in outputs:
            mult = self._study.dataset_spec(output).multiplicity
            self._outputs[mult].append(output)
        self._outputnodes = {}
        for mult in self._outputs:
            self._outputnodes[mult] = pe.Node(
                IdentityInterface(
                    fields=[o.name for o in self._outputs[mult]]),
                name="{}_{}_outputnode".format(name, mult))
        # Create sets of unconnected inputs/outputs
        self._unconnected_inputs = set(self.input_names)
        self._unconnected_outputs = set(self.output_names)
        assert len(inputs) == len(self._unconnected_inputs), (
            "Duplicate inputs found in '{}'"
            .format("', '".join(self.input_names)))
        assert len(outputs) == len(self._unconnected_outputs), (
            "Duplicate outputs found in '{}'"
            .format("', '".join(self.output_names)))
        self._citations = citations
        self._default_options = default_options
        # Copy default options to options and then update it with specific
        # options passed to this pipeline
        self._options = copy(default_options)
        for k, v in options.iteritems():
            if k in self.options:
                self.options[k] = v
        self._description = description
        self._requirements = requirements
        self._approx_runtime = approx_runtime
        self._min_nthreads = min_nthreads
        self._max_nthreads = max_nthreads

    def _check_spec_names(self, specs, spec_type):
        # Check for unrecognised inputs/outputs
        unrecognised = set(s for s in specs
                           if s.name not in self.study.dataset_spec_names())
        if unrecognised:
            raise NiAnalysisError(
                "'{}' are not valid {} names for {} study ('{}')"
                .format("', '".join(u.name for u in unrecognised), spec_type,
                        self.study.__class__.__name__,
                        "', '".join(self.study.dataset_spec_names())))

    def __repr__(self):
        return "Pipeline(name='{}')".format(self.name)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        # NB: Workflows should be the same for pipelines of the same name so
        #     may not need to be checked.
        return (
            self._name == other._name and
            self._study == other._study and
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
            reprocess=False):
        """
        Connects pipeline to archive and runs it

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
        """
        complete_workflow = pe.Workflow(name=self.name, base_dir=work_dir)
        self._connect_to_archive(complete_workflow, subject_ids, session_ids,
                                 reprocess)
        # Run the workflow
        return complete_workflow.run()

    def _connect_to_archive(self, complete_workflow, subject_ids,
                            session_ids, reprocess, project=None):
        """
        Gets a data source and data sink from the archive for the requested
        sessions, connects them to the pipeline's NiPyPE workflow

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
            runs of prerequisite pipelines to avoid having to re-query the
            archive. If None, the study info is loaded from the study
            archive.
        """
        # Check all inputs and outputs are connected
        self.assert_connected()
        # Get list of available subjects and their associated sessions/datasets
        # from the archive
        if project is None:
            project = self._study.archive.project(
                self._study._project_id, subject_ids=subject_ids,
                session_ids=session_ids)
        # Get list of sessions and subjects that need to be processed (i.e. if
        # they don't contain the outputs of this pipeline)
        sessions_to_process, subjects_to_process = self.to_process(project,
                                                                   reprocess)
        if not sessions_to_process and not subjects_to_process:
            logger.info(
                "All outputs of '{}' are already present in project archive, "
                "skipping".format(self.name))
            return None
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow.add_nodes([self._workflow])
        sessions, subjects = self._get_sessions_node(
            session_ids, sessions_to_process, subjects_to_process,
            complete_workflow)
        # Prepend prerequisite pipelines to complete workflow
        for prereq in self.prerequisities:
            # NB: Even if reprocess==True, the prerequisite pipelines are not
            #     re-processed, they are only reprocessed if reprocess == 'all'
            prereq_summary = prereq._connect_to_archive(
                complete_workflow, [s.id for s in subjects_to_process],
                session_ids, (reprocess if reprocess == 'all' else False),
                project)
            # Connect the output summary of the prerequisite to the pipeline
            # to ensure that the prerequisite is run first.
            complete_workflow.connect(
                prereq_summary, 'sessions', sessions,
                prereq.name + '_sessions')
        try:
            # Create source and sinks from the archive
            source = self._study.archive.source(
                self.study.project_id,
                (self.study.dataset(i) for i in self.inputs),
                study_name=self.study.name)
        except NiAnalysisMissingDatasetError as e:
            raise NiAnalysisMissingDatasetError(
                str(e) + ", which is required for pipeline '{}'".format(
                    self.name))
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(sessions, 'subject_id',
                                  source, 'subject_id')
        complete_workflow.connect(sessions, 'session_id',
                                  source, 'session_id')
        for inpt in self.inputs:
            # Get the dataset corresponding to the pipeline's input
            dataset = self.study.dataset(inpt.name)
            if dataset.format != inpt.format:
                # Insert a format converter node into the workflow if the
                # format of the dataset if it is not in the required format for
                # the study
                conv_node_name = inpt.name + '_input_conversion'
                dataset_source, dataset_name = get_converter_node(
                    dataset, dataset.name + OUTPUT_SUFFIX, inpt.format,
                    source, complete_workflow, conv_node_name)
            else:
                dataset_source = source
                dataset_name = dataset.name + OUTPUT_SUFFIX
            # Connect the dataset to the pipeline input
            complete_workflow.connect(dataset_source, dataset_name,
                                      self.inputnode, inpt.name)
        # Create a summary node for holding a summary of all the sessions/
        # subjects that were sunk. This is used to connect with dependent
        # pipelines into one large connected pipeline.
        output_summary = pe.Node(OutputSummary(),
                                 name=self.name + '_output_summary')
        # Connect all outputs to the archive sink
        for mult, outputs in self._outputs.iteritems():
            # Create a new sink for each multiplicity level (i.e 'per_session',
            # 'per_subject' or 'per_project')
            sink = self.study.archive.sink(
                self.study._project_id,
                (self.study.dataset(o) for o in outputs), mult,
                study_name=self.study.name)
            sink.inputs.description = self.description
            sink.inputs.name = self._study.name
            if mult != 'per_project':
                complete_workflow.connect(sessions, 'subject_id',
                                          sink, 'subject_id')
                if mult == 'per_session':
                    complete_workflow.connect(sessions, 'session_id',
                                              sink, 'session_id')
            for output in outputs:
                # Get the dataset spec corresponding to the pipeline's output
                dataset = self.study.dataset(output.name)
                # Skip datasets which are already input datasets
                if dataset.processed:
                    # Convert the format of the node if it doesn't match
                    if dataset.format != output.format:
                        conv_node_name = output.name + '_output_conversion'
                        output_node, node_dataset_name = get_converter_node(
                            output, output.name, dataset.format,
                            self._outputnodes[mult], complete_workflow,
                            conv_node_name)
                    else:
                        output_node = self._outputnodes[mult]
                        node_dataset_name = dataset.name
                    complete_workflow.connect(
                        output_node, node_dataset_name,
                        sink, dataset.name + INPUT_SUFFIX)
            if mult == 'per_session':
                session_output_summary = pe.JoinNode(
                    IdentityInterface(fields=['sessions']),
                    joinsource=sessions, joinfield='sessions',
                    name=self.name + '_session_output_summary')
                complete_workflow.connect(sink, 'sessions',
                                          session_output_summary, 'sessions')
                complete_workflow.connect(session_output_summary, 'sessions',
                                          output_summary, 'sessions')
            elif mult == 'per_subject':
                assert subjects is not None
                subject_output_summary = pe.JoinNode(
                    IdentityInterface(fields=['subjects']),
                    joinsource=subjects, joinfield='subjects',
                    name=self.name + '_session_output_summary')
                complete_workflow.connect(sink, 'subjects',
                                          subject_output_summary, 'subjects')
                complete_workflow.connect(subject_output_summary, 'subjects',
                                          output_summary, 'subjects')
            elif mult == 'per_project':
                complete_workflow.connect(sink, 'project',
                                          output_summary, 'project')
        return output_summary

    def _get_sessions_node(self, session_ids, sessions_to_process,
                           subjects_to_process, complete_workflow):
        """
        Generate an input node that iterates over the sessions and subjects
        that need to be processed.
        """
        sessions = pe.Node(InputSessions(), name='sessions')
        complete_workflow.add_nodes([sessions])
        # Set up session/subject "iterables" to control the iteration of the
        # pipeline over the project
        if any(self.study.dataset_spec(o).multiplicity != 'per_session'
               for o in self.outputs):
            # If subject or study outputs iterate through subjects and
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
            subjects = None
        return sessions, subjects

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
            comp = self._study.dataset(input)
            if comp.processed:
                pipelines.add(comp.pipeline)
        # Call pipeline instancemethods to study with provided options
        return (p(self._study, **self.options) for p in pipelines)

    def to_process(self, project, reprocess=False):
        """
        Check whether the outputs of the pipeline are present in all sessions
        in the project archive, and make a list of the sessions and subjects
        that need to be reprocessed if they aren't.

        Parameters
        ----------
        project : Project
            A representation of the project and associated subjects and
            sessions for the study's archive.
        """
        all_subjects = list(project.subjects)
        all_sessions = list(chain(*[s.sessions for s in project.subjects]))
        if reprocess:
            return all_sessions, all_subjects
        sessions_to_process = set()
        subjects_to_process = set()
        for output in self.outputs:
            dataset = self.study.dataset(output)
            # If there is a project output then all subjects and sessions need
            # to be reprocessed
            if dataset.multiplicity == 'per_project':
                if dataset.prefixed_name not in project.dataset_names:
                    return all_sessions, all_subjects
            elif dataset.multiplicity == 'per_subject':
                subjects_to_process.update(
                    s for s in all_subjects
                    if dataset.prefixed_name not in s.dataset_names)
            elif dataset.multiplicity == 'per_session':
                sessions_to_process.update(
                    s for s in all_sessions
                    if dataset.prefixed_name not in s.dataset_names)
            else:
                assert False, "Unrecognised multiplicity of {}".format(dataset)
        subjects_to_process.update(
            s.subject for s in sessions_to_process)
        return list(sessions_to_process), list(subjects_to_process)

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

    def connect_input(self, spec_name, node, node_input, join=None):
        """
        Connects a study dataset_spec as an input to the provided node

        Parameters
        ----------
        spec_name : str
            Name of the study dataset spec to join to the node
        node : nipype.pipeline.BaseNode
            A NiPype node to connect the input to
        node_input : str
            Name of the input on the node to connect the dataset spec to
        join : str  (not implemented)  # TODO
            Whether to join the input over sessions, subjects or the whole
            study. Can be one of:
              'sessions'     - Sessions for each subject are joined into a
                               list
              'subjects'     - Subjects for each session are joined into a
                               list
              'project'      - Sessions for each are joined into a list
                               and then nested in a list over all subjects
              'project_flat' - All sessions across all subjects are joined
                               into a single list
        """
        assert spec_name in self.input_names, (
            "'{}' is not a valid input for '{}' pipeline ('{}')"
            .format(spec_name, self.name, "', '".join(self._inputs)))
        if join is not None:
            join_name = '{}_{}_{}_join_'.format(spec_name, node.name,
                                                node_input)
            if join.startswith('project'):
                # Create node to join the sessions first
                session_join = pe.JoinNode(
                    IdentityInterface([spec_name]),
                    name='session_' + join_name,
                    joinsource='sessions', joinfield=[spec_name])
                if join == 'project':
                    inputnode = pe.JoinNode(
                        IdentityInterface([spec_name]),
                        name='subject_' + join_name,
                        joinsource='subjects', joinfield=[spec_name])
                elif join == 'project_flat':
                    # TODO: Need to implemente Chain interface for
                    # concatenating the session lists into a single list
                    inputnode = pe.JoinNode(
                        Chain([spec_name]), name='subject_' + join_name,
                        joinsource='subjects', joinfield=[spec_name])
                else:
                    raise NiAnalysisError(
                        "Unrecognised join command '{}' can be one of ("
                        "'sessions', 'subjects', 'project', 'project_flat')"
                        .format(join))
                self._workflow.connect(self._inputnode, spec_name,
                                       session_join, spec_name)
                self._workflow.connect(session_join, spec_name, inputnode,
                                       spec_name)
            else:
                inputnode = pe.JoinNode(
                    IdentityInterface([spec_name]), name=join_name,
                    joinsource=join, joinfield=[spec_name])
                self._workflow.connect(self._inputnode, spec_name, inputnode,
                                       spec_name)
        else:
            inputnode = self._inputnode
        self._workflow.connect(inputnode, spec_name, node, node_input)
        if spec_name in self._unconnected_inputs:
            self._unconnected_inputs.remove(spec_name)

    def connect_output(self, spec_name, node, node_output):
        """
        Connects an output to a study dataset spec

        Parameters
        ----------
        spec_name : str
            Name of the study dataset spec to connect to
        node : nipype.pipeline.BaseNode
            A NiPype to connect the output from
        node_output : str
            Name of the output on the node to connect to the dataset
        """
        assert spec_name in self.output_names, (
            "'{}' is not a valid output for '{}' pipeline ('{}')"
            .format(spec_name, self.name, "', '".join(self.output_names)))
        assert spec_name in self._unconnected_outputs, (
            "'{}' output has been connected already")
        outputnode = self._outputnodes[
            self._study.dataset_spec(spec_name).multiplicity]
        self._workflow.connect(node, node_output, outputnode, spec_name)
        self._unconnected_outputs.remove(spec_name)

    @property
    def name(self):
        return self._name

    @property
    def study(self):
        return self._study

    @property
    def workflow(self):
        return self._workflow

    @property
    def version(self):
        return self._version

    @property
    def inputs(self):
        return iter(self._inputs)

    @property
    def outputs(self):
        return chain(*self._outputs.values())

    @property
    def input_names(self):
        return (i.name for i in self.inputs)

    @property
    def output_names(self):
        return (o.name for o in self.outputs)

    @property
    def default_options(self):
        return self._default_options

    @property
    def options(self):
        return self._options

    def option(self, name):
        return self._options[name]

    @property
    def non_default_options(self):
        return ((k, v) for k, v in self.options.iteritems()
                if v != self.default_options[k])

    @property
    def description(self):
        return self._description

    @property
    def inputnode(self):
        return self._inputnode

    def outputnode(self, multiplicity):
        """
        Returns the output node for the given multiplicity

        Parameters
        ----------
        multiplicity : str
            The multiplicity of the output node. Can be 'per_session',
            'per_subject' or 'per_project'
        """
        return self._outputnodes[multiplicity]

    @property
    def mutliplicities(self):
        "The multiplicities present in the pipeline outputs"
        return self._outputs.iterkeys()

    def multiplicity_outputs(self, mult):
        return iter(self._outputs[mult])

    def multiplicity_output_names(self, mult):
        return (o.name for o in self.multiplicity_outputs(mult))

    def multiplicity(self, output):
        mults = [m for m, outputs in self._outputs.itervalues()
                 if output in outputs]
        if not mults:
            raise KeyError(
                "'{}' is not an output of pipeline '{}'".format(output,
                                                                self.name))
        else:
            assert len(mults) == 1
            mult = mults[0]
        return mult

    @property
    def approx_runtime(self):
        return self._approx_runtime

    @property
    def citations(self):
        return self._citations

    @property
    def requirements(self):
        return self._requirements

    @property
    def min_nthreads(self):
        return self._min_nthreads

    @property
    def max_nthreads(self):
        return self._max_nthreads

    def node(self, name):
        return self.workflow.get_node(name)

    @property
    def suffix(self):
        """
        A suffixed appended to output filenames when they are archived to
        identify the options used to generate them
        """
        return '__'.join('{}_{}'.format(k, v)
                         for k, v in self.options.iteritems())

    def add_input(self, input_name):
        """
        Adds a new input to the pipeline. Useful if extending a pipeline in a
        derived Study class

        Parameters
        ----------
        input_name : str
            Name of the input to add to the pipeline
        """
        if input_name not in self.study.dataset_spec_names():
            raise NiAnalysisDatasetNameError(
                "'{}' is not a name of a dataset_spec in {} Studys"
                .format(input_name, self.study.name))
        self._inputs.append(input_name)

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
