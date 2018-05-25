import os
import tempfile
import shutil
from itertools import chain
from collections import defaultdict
from nipype.pipeline import engine as pe
import errno
from .node import Node, JoinNode, MapNode
from nipype.interfaces.utility import IdentityInterface
from arcana.interfaces.utils import Merge
from logging import getLogger
from arcana.exception import (
    ArcanaNameError, ArcanaError, ArcanaMissingDataException,
    ArcanaNoRunRequiredException,
    ArcanaNoConverterError, ArcanaOutputNotProducedException)
from arcana.dataset.base import BaseDataset, BaseField
from arcana.interfaces.iterators import (
    InputSessions, PipelineReport, InputSubjects, SubjectReport,
    VisitReport, SubjectSessionReport, SessionReport)
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX


logger = getLogger('Arcana')


class Pipeline(object):
    """
    A fairly thin wrapper around a NiPype workflow, which keeps track
    inputs and outputs, and maps names of nodes to avoid name-clashes

    Parameters
    ----------
    name : str
        The name of the pipeline
    study : Study
        The study from which the pipeline was created
    inputs : List[DatasetSpec|FieldSpec]
        The list of input datasets required for the pipeline
        un/processed datasets, and the options used to generate them for
        unprocessed datasets
    outputs : List[DatasetSpec|FieldSpec]
        The list of outputs (hard-coded names for un/processed datasets)
    desc : str
        The description of what the pipeline does
    citations : List[Citation]
        List of citations that describe the workflow and should be cited in
        publications
    version : int
        A version number for the pipeline to be incremented whenever the output
        of the pipeline
    name_prefix : str
        Prefix prepended to the name of the pipeline. Typically passed
        in from a kwarg of the pipeline constructor method to allow
        multi-classes to alter the name of the pipeline to avoid name
        clashes
    add_inputs : List[DatasetSpec|FieldSpec]
        Additional inputs to append to the inputs argument. Typically
        passed in from a kwarg of the pipeline constructor method to
        allow sub-classes to add additional inputs
    add_outputs : List[DatasetSpec|FieldSpec]
        Additional outputs to append to the outputs argument. Typically
        passed in from a kwarg of the pipeline constructor method to
        allow sub-classes to add additional outputs
    """

    iterfields = ('subject_id', 'visit_id')

    def __init__(self, study, name, inputs, outputs, desc,
                 citations, version, name_prefix='',
                 add_inputs=[], add_outputs=[]):
        self._name = name_prefix + name
        inputs = list(inputs) + list(add_inputs)
        outputs = list(outputs) + list(add_outputs)
        self._study = study
        self._workflow = pe.Workflow(name=self.name)
        self._version = int(version)
        self._desc = desc
        # Set up inputs
        self._check_spec_names(inputs, 'input')
        if any(i.name in self.iterfields for i in inputs):
            raise ArcanaError(
                "Cannot have a dataset spec named '{}' as it clashes with "
                "iterable field of that name".format(i.name))
        self._inputs = inputs
        self._inputnode = self.create_node(
            IdentityInterface(fields=(
                tuple(self.input_names) + self.iterfields)),
            name="inputnode", wall_time=10, memory=1000)
        # Set up outputs
        self._check_spec_names(outputs, 'output')
        self._outputs = defaultdict(list)
        for output in outputs:
            freq = self._study.data_spec(output).frequency
            self._outputs[freq].append(output)
        self._outputnodes = {}
        for freq in self._outputs:
            self._outputnodes[freq] = self.create_node(
                IdentityInterface(
                    fields=[o.name for o in self._outputs[freq]]),
                name="{}_outputnode".format(freq), wall_time=10,
                memory=1000)
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
        # Keep record of all options used in the pipeline construction
        # so that they can be saved with the provenence.
        self._used_options = set()

    def _check_spec_names(self, specs, spec_type):
        # Check for unrecognised inputs/outputs
        unrecognised = set(s for s in specs
                           if s.name not in self.study.data_spec_names())
        if unrecognised:
            raise ArcanaError(
                "'{}' are not valid {} names for {} study ('{}')"
                .format("', '".join(u.name for u in unrecognised), spec_type,
                        self.study.__class__.__name__,
                        "', '".join(self.study.data_spec_names())))

    def __repr__(self):
        return "{}(name='{}')".format(self.__class__.__name__,
                                      self.name)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._name == other._name and
            self._study == other._study and
            self._desc == other._desc and
            self._version == other.version and
            self._inputs == other._inputs and
            self._outputs == other._outputs and
            self._citations == other._citations)

    def __ne__(self, other):
        return not (self == other)

    def connect_to_archive(self, complete_workflow, subject_ids=None,
                           visit_ids=None, reprocess=False,
                           connected_prereqs=None):
        """
        Gets a data source and data sink from the archive for the requested
        sessions, connects them to the pipeline's NiPyPE workflow

        Parameters
        ----------
        subject_ids : List[str]
            The subset of subject IDs to process. If None all available will be
            reprocessed
        visit_ids: List[str]
            The subset of visit IDs for each subject to process. If None all
            available will be reprocessed
        work_dir : str
            A directory in which to run the nipype workflows
        reprocess: True|False|'all'
            A flag which determines whether to rerun the processing for this
            step. If set to 'all' then pre-requisite pipelines will also be
            reprocessed.
        connected_prereqs: list(Pipeline, Node)
            Prerequisite pipelines that have already been connected to the
            workflow (prequisites of prerequisites) and their corresponding
            "report" nodes

        Returns
        -------
        report : ReportNode
            The final report node, which can be connected to subsequent
            pipelines
        """
        if connected_prereqs is None:
            connected_prereqs = {}
        # Check all inputs and outputs are connected
        self.assert_connected()
        # Get list of sessions that need to be processed (i.e. if
        # they don't contain the outputs of this pipeline)
        sessions_to_process = self._sessions_to_process(
            subject_ids=subject_ids, visit_ids=visit_ids,
            reprocess=reprocess)
        if not sessions_to_process:
            raise ArcanaNoRunRequiredException(
                "All outputs of '{}' are already present in project archive, "
                "skipping".format(self.name))
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow.add_nodes([self._workflow])
        # Get iterator nodes over subjects and sessions to be processed
        subjects, sessions = self._subject_and_session_iterators(
            sessions_to_process, complete_workflow)
        # Prepend prerequisite pipelines to complete workflow if required
        if self.has_prerequisites:
            reports = []
            prereq_subject_ids = list(
                set(s.subject.id for s in sessions_to_process))
            for prereq in self.prerequisites:
                try:
                    (connected_prereq,
                     prereq_report) = connected_prereqs[prereq.name]
                    if connected_prereq != prereq:
                        raise ArcanaError(
                            "Name clash between {} and {} non-matching "
                            "prerequisite pipelines".format(connected_prereq,
                                                            prereq))
                    reports.append(prereq_report)
                except KeyError:
                    # NB: Even if reprocess==True, the prerequisite pipelines
                    # are not re-processed, they are only reprocessed if
                    # reprocess == 'all'
                    try:
                        prereq_report = prereq.connect_to_archive(
                            complete_workflow=complete_workflow,
                            subject_ids=prereq_subject_ids,
                            visit_ids=visit_ids,
                            reprocess=(
                                reprocess
                                if reprocess == 'all' else False),
                            connected_prereqs=connected_prereqs)
                        if prereq_report is not None:
                            connected_prereqs[prereq.name] = (
                                prereq, prereq_report)
                            reports.append(prereq_report)
                    except ArcanaNoRunRequiredException:
                        logger.info(
                            "Not running '{}' pipeline as a "
                            "prerequisite of '{}' as the required "
                            "outputs are already present in the archive"
                            .format(prereq.name, self.name))
            if reports:
                prereq_reports = self.create_node(Merge(len(reports)),
                                                  'prereq_reports')
                for i, report in enumerate(reports, 1):
                    # Connect the output summary of the prerequisite to the
                    # pipeline to ensure that the prerequisite is run first.
                    complete_workflow.connect(
                        report, 'subject_session_pairs',
                        prereq_reports, 'in{}'.format(i))
                complete_workflow.connect(prereq_reports, 'out', subjects,
                                          'prereq_reports')
        try:
            # Create source and sinks from the archive
            source = self._study.archive.source(
                (self.study.spec(i) for i in self.inputs),
                study_name=self.study.name,
                name='{}_source'.format(self.name))
        except ArcanaMissingDataException as e:
            raise ArcanaMissingDataException(
                str(e) + ", which is required for pipeline '{}'".format(
                    self.name))
        # Map the subject and visit IDs to the input node of the pipeline
        # for use in connect_subject_id and connect_visit_id
        complete_workflow.connect(sessions, 'subject_id',
                                  self.inputnode, 'subject_id')
        complete_workflow.connect(sessions, 'visit_id',
                                  self.inputnode, 'visit_id')
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(sessions, 'subject_id',
                                  source, 'subject_id')
        complete_workflow.connect(sessions, 'visit_id',
                                  source, 'visit_id')
        for input_spec in self.inputs:
            # Get the dataset corresponding to the pipeline's input
            input = self.study.spec(input_spec.name)  # @ReservedAssignment @IgnorePep8
            if isinstance(input, BaseDataset):
                if input.format != input_spec.format:
                    # Insert a format converter node into the workflow if the
                    # format of the dataset if it is not in the required format
                    # for the study
                    try:
                        converter = input_spec.format.converter_from(
                            input.format)
                    except ArcanaNoConverterError as e:
                        raise ArcanaNoConverterError(
                            str(e) + (
                                " required to convert {} to {} "
                                " in '{}' pipeline, in study '{}"
                                .format(input.name, input_spec.name,
                                        self.name, self.study.name)))
                    conv_node_name = '{}_{}_input_conversion'.format(
                        self.name, input_spec.name)
                    (dataset_source, conv_in_field,
                     dataset_name) = converter.get_node(conv_node_name)
                    complete_workflow.connect(
                        source, input.name + PATH_SUFFIX,
                        dataset_source, conv_in_field)
                else:
                    dataset_source = source
                    dataset_name = input.name + PATH_SUFFIX
                # Connect the dataset to the pipeline input
                complete_workflow.connect(dataset_source, dataset_name,
                                          self.inputnode, input_spec.name)
            else:
                assert isinstance(input, BaseField)
                complete_workflow.connect(
                    source, input.name + FIELD_SUFFIX,
                    self.inputnode, input_spec.name)
        # Create a report node for holding a summary of all the sessions/
        # subjects that were sunk. This is used to connect with dependent
        # pipelines into one large connected pipeline.
        report = self.create_node(PipelineReport(), 'report')
        # Connect all outputs to the archive sink
        for freq, outputs in self._outputs.iteritems():
            # Create a new sink for each frequency level (i.e 'per_session',
            # 'per_subject', 'per_visit', or 'per_project')
            sink = self.study.archive.sink(
                (self.study.spec(o) for o in outputs),
                frequency=freq,
                study_name=self.study.name,
                name='{}_{}_sink'.format(self.name, freq))
#             sink.inputs.desc = self.desc
#             sink.inputs.name = self._study.name
            if freq in ('per_session', 'per_subject'):
                complete_workflow.connect(sessions, 'subject_id',
                                          sink, 'subject_id')
            if freq in ('per_session', 'per_visit'):
                complete_workflow.connect(sessions, 'visit_id',
                                          sink, 'visit_id')
            for output_spec in outputs:
                # Get the dataset spec corresponding to the pipeline's output
                output = self.study.spec(output_spec.name)
                # Skip datasets which are already input datasets
                if output.is_spec:
                    if isinstance(output, BaseDataset):
                        # Convert the format of the node if it doesn't match
                        if output.format != output_spec.format:
                            try:
                                converter = output.format.converter_from(
                                    output_spec.format)
                            except ArcanaNoConverterError as e:
                                raise ArcanaNoConverterError(
                                    str(e) + (
                                        " required to convert {} to {} "
                                        " in '{}' pipeline, in study '{}"
                                        .format(
                                            input.name, input_spec.name,
                                            self.name, self.study.name)))
                            conv_node_name = (output_spec.name +
                                              '_output_conversion')
                            (output_node, conv_in_field,
                             node_dataset_name) = converter.get_node(
                                 conv_node_name)
                            complete_workflow.connect(
                                self._outputnodes[freq],
                                output_spec.name,
                                output_node, conv_in_field)
                        else:
                            output_node = self._outputnodes[freq]
                            node_dataset_name = output.name
                        complete_workflow.connect(
                            output_node, node_dataset_name,
                            sink, output.name + PATH_SUFFIX)
                    else:
                        assert isinstance(output, BaseField)
                        complete_workflow.connect(
                            self._outputnodes[freq], output.name, sink,
                            output.name + FIELD_SUFFIX)
            self._connect_to_reports(
                sink, report, freq, subjects, sessions, complete_workflow)
        return report

    def _subject_and_session_iterators(self, sessions_to_process, workflow):
        """
        Generate an input node that iterates over the sessions and subjects
        that need to be processed.
        """
        # Create nodes to control the iteration over subjects and sessions in
        # the project
        subjects = self.create_node(InputSubjects(), 'subjects', wall_time=10,
                                    memory=1000)
        sessions = self.create_node(InputSessions(), 'sessions', wall_time=10,
                                    memory=4000)
        # Construct iterable over all subjects to process
        subjects_to_process = set(s.subject for s in sessions_to_process)
        subject_ids_to_process = set(s.id for s in subjects_to_process)
        subjects.iterables = ('subject_id',
                              tuple(s.id for s in subjects_to_process))
        # Determine whether the visit ids are the same for every subject,
        # in which case they can be set as a constant, otherwise they will
        # need to be specified for each subject separately
        session_subjects = defaultdict(set)
        for session in sessions_to_process:
            session_subjects[session.visit_id].add(session.subject_id)
        if all(ss == subject_ids_to_process
               for ss in session_subjects.itervalues()):
            # All sessions are to be processed in every node, a simple second
            # layer of iterations on top of the subject iterations will
            # suffice. This allows re-combining on visit_id across subjects
            sessions.iterables = ('visit_id', session_subjects.keys())
        else:
            # visit IDs to be processed vary between subjects and so need
            # to be specified explicitly
            subject_sessions = defaultdict(list)
            for session in sessions_to_process:
                subject_sessions[session.subject.id].append(session.visit_id)
            sessions.itersource = ('{}_subjects'.format(self.name),
                                   'subject_id')
            sessions.iterables = ('visit_id', subject_sessions)
        # Connect subject and session nodes together
        workflow.connect(subjects, 'subject_id', sessions, 'subject_id')
        return subjects, sessions

    def _connect_to_reports(self, sink, output_summary, freq, subjects,
                            sessions, workflow):
        """
        Connects the sink of the pipeline to an "Output Summary", which lists
        the subjects and sessions that were processed for the pipeline. There
        should be only one summary node instance per pipeline so it can be
        used to feed into the input of subsequent pipelines to ensure that
        they are executed afterwards.
        """
        if freq == 'per_session':
            session_outputs = JoinNode(
                SessionReport(), joinsource=sessions,
                joinfield=['subjects', 'sessions'],
                name=self.name + '_session_outputs', wall_time=20,
                memory=4000)
            subject_session_outputs = JoinNode(
                SubjectSessionReport(), joinfield='subject_session_pairs',
                joinsource=subjects,
                name=self.name + '_subject_session_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'subject_id', session_outputs, 'subjects')
            workflow.connect(sink, 'visit_id', session_outputs, 'sessions')
            workflow.connect(session_outputs, 'subject_session_pairs',
                             subject_session_outputs, 'subject_session_pairs')
            workflow.connect(
                subject_session_outputs, 'subject_session_pairs',
                output_summary, 'subject_session_pairs')
        elif freq == 'per_subject':
            subject_output_summary = JoinNode(
                SubjectReport(), joinsource=subjects, joinfield='subjects',
                name=self.name + '_subject_summary_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'subject_id',
                             subject_output_summary, 'subjects')
            workflow.connect(subject_output_summary, 'subjects',
                             output_summary, 'subjects')
        elif freq == 'per_visit':
            visit_output_summary = JoinNode(
                VisitReport(), joinsource=sessions, joinfield='sessions',
                name=self.name + '_visit_summary_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'visit_id',
                             visit_output_summary, 'sessions')
            workflow.connect(visit_output_summary, 'sessions',
                             output_summary, 'visits')
        elif freq == 'per_project':
            workflow.connect(sink, 'project_id', output_summary, 'project')

    @property
    def has_prerequisites(self):
        return any(self._study.spec(i).is_spec for i in self.inputs)

    @property
    def prerequisites(self):
        """
        Iterate through all prerequisite pipelines
        """
        # Loop through the inputs to the pipeline and add the instancemethods
        # for the pipelines to generate each of the processed inputs
        pipeline_getters = set()
        required_outputs = defaultdict(set)
        for input in self.inputs:  # @ReservedAssignment
            spec = self._study.spec(input)
            # Could be an input to the study or optional acquired spec
            if spec.is_spec and spec.derived:
                pipeline_getters.add(spec.pipeline)
                required_outputs[spec.pipeline].add(input.name)
        # Call pipeline-getter instance method on study with provided options
        # to generate pipeline to run
        for getter in pipeline_getters:
            pipeline = getter()
            # Check that the required outputs are created with the given
            # options
            missing_outputs = required_outputs[getter] - set(
                d.name for d in pipeline.outputs)
            if missing_outputs:
                raise ArcanaOutputNotProducedException(
                    "Output(s) '{}', required for '{}' pipeline, will "
                    "not be created by prerequisite pipeline '{}' "
                    "with options: {}".format(
                        "', '".join(missing_outputs), self.name,
                        pipeline.name,
                        '\n'.join('{}={}'.format(o.name, o.value)
                                  for o in self.study.options)))
            yield pipeline

    @property
    def study_inputs(self):
        """
        Returns all inputs to the pipeline, including inputs of
        prerequisites (and their prerequisites recursively)
        """
        return chain((i for i in self.inputs
                      if not self._study.data_spec(i).derived),
                     *(p.study_inputs for p in self.prerequisites))

    def _sessions_to_process(self, subject_ids=None, visit_ids=None,
                             reprocess=False):
        """
        Check whether the outputs of the pipeline are present in all sessions
        in the project archive, and make a list of the sessions and subjects
        that need to be reprocessed if they aren't.

        Parameters
        ----------
        subject_ids : list(str)
            Filter the subject IDs to process
        visit_ids : list(str)
            Filter the visit IDs to process
        reprocess : bool
            Whether to reprocess the pipeline outputs even if they
            exist.
        """
        # Get list of available subjects and their associated sessions/datasets
        # from the archive
        def filter_sessions(sessions):  # @IgnorePep8
            if visit_ids is None and subject_ids is None:
                return sessions
            return (
                s for s in sessions
                if ((visit_ids is None or s.visit_id in visit_ids) and
                    (subject_ids is None or s.subject_id in subject_ids)))
        tree = self._study.tree
        subjects = ([s for s in tree.subjects if s.id in subject_ids]
                    if subject_ids is not None else list(tree.subjects))
        visits = ([v for v in tree.visits if s.id in visit_ids]
                    if visit_ids is not None else list(tree.visits))
        # Get all filtered sessions
        all_sessions = list(chain(*[filter_sessions(s.sessions)
                                    for s in subjects]))
        if reprocess:
            return all_sessions
        sessions_to_process = set()
        for output_spec in self.outputs:
            output = self.study.spec(output_spec)
            # If there is a project output then all subjects and sessions need
            # to be reprocessed
            if output.frequency == 'per_project':
                if output.prefixed_name not in tree.data_names:
                    # Return all filtered sessions
                    return all_sessions
            elif output.frequency == 'per_subject':
                sessions_to_process.update(chain(*(
                    filter_sessions(s.sessions) for s in subjects
                    if output.prefixed_name not in s.data_names)))
            elif output.frequency == 'per_visit':
                sessions_to_process.update(chain(*(
                    filter_sessions(v.sessions) for v in visits
                    if (output.prefixed_name not in v.data_names))))
            elif output.frequency == 'per_session':
                sessions_to_process.update(filter_sessions(
                    s for s in all_sessions
                    if output.prefixed_name not in s.all_data_names))
            else:
                assert False, ("Unrecognised frequency of {}"
                               .format(output))
        return list(sessions_to_process)

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

    def save_graph(self, fname, style='flat', complete=False):
        """
        Saves a graph of the pipeline to file

        Parameters
        ----------
        fname : str
            The filename for the saved graph
        style : str
            The style of the graph, can be one of can be one of
            'orig', 'flat', 'exec', 'hierarchical'
        complete : bool
            Whether to plot the complete graph including sources, sinks and
            prerequisite pipelines or just the current pipeline
        plot : bool
            Whether to load and plot the graph after it has been written
        """
        fname = os.path.expanduser(fname)
        orig_dir = os.getcwd()
        tmpdir = tempfile.mkdtemp()
        os.chdir(tmpdir)
        if complete:
            workflow = pe.Workflow(name=self.name, base_dir=tmpdir)
            self.connect_to_archive(workflow)
            out_dir = os.path.join(tmpdir, self.name)
        else:
            workflow = self._workflow
            out_dir = tmpdir
        workflow.write_graph(graph2use=style)
        os.chdir(orig_dir)
        try:
            shutil.move(os.path.join(out_dir, 'graph_detailed.png'),
                        fname)
        except IOError as e:
            if e.errno == errno.ENOENT:
                shutil.move(os.path.join(out_dir, 'graph.png'), fname)
            else:
                raise
        shutil.rmtree(tmpdir)

    def connect_input(self, spec_name, node, node_input):
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
        """
        assert spec_name in self.input_names, (
            "'{}' is not a valid input for '{}' pipeline ('{}')"
            .format(spec_name, self.name, "', '".join(str(i)
                                                      for i in self._inputs)))
        self._workflow.connect(self._inputnode, spec_name, node, node_input)
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
            self._study.data_spec(spec_name).frequency]
        self._workflow.connect(node, node_output, outputnode, spec_name)
        self._unconnected_outputs.remove(spec_name)

    def connect_subject_id(self, node, node_input):
        """
        Connects the subject ID from the input node of the pipeline to an
        internal node

        Parameters
        ----------
        node : BaseNode
            The node to connect the subject ID to
        node_input : str
            The name of the field of the node to connect the subject ID to
        """
        self._workflow.connect(self._inputnode, 'subject_id', node, node_input)

    def connect_visit_id(self, node, node_input):
        """
        Connects the visit ID from the input node of the pipeline to an
        internal node

        Parameters
        ----------
        node : BaseNode
            The node to connect the subject ID to
        node_input : str
            The name of the field of the node to connect the subject ID to
        """
        self._workflow.connect(self._inputnode, 'visit_id', node, node_input)

    def create_node(self, interface, name, **kwargs):
        """
        Creates a Node in the pipeline (prepending the pipeline namespace)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        name : str
            Name for the node
        requirements : list(Requirement)
            List of required packages need for the node to run (default: [])
        wall_time : float
            Time required to execute the node in minutes (default: 1)
        memory : int
            Required memory for the node in MB (default: 1000)
        nthreads : int
            Preferred number of threads to run the node on (default: 1)
        gpu : bool
            Flags whether a GPU compute node is preferred or not
            (default: False)
        account : str
            Name of the account to submit slurm scripts to
        """
        node = Node(interface, name="{}_{}".format(self._name, name), **kwargs)
        self._workflow.add_nodes([node])
        return node

    def create_map_node(self, interface, name, **kwargs):
        """
        Creates a MapNode in the pipeline (prepending the pipeline namespace)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        name : str
            Name for the node
        requirements : list(Requirement)
            List of required packages need for the node to run (default: [])
        wall_time : float
            Time required to execute the node in minutes (default: 1)
        memory : int
            Required memory for the node in MB (default: 1000)
        nthreads : int
            Preferred number of threads to run the node on (default: 1)
        gpu : bool
            Flags whether a GPU compute node is preferred or not
            (default: False)
        account : str
            Name of the account to submit slurm scripts to
        """
        node = MapNode(interface, name="{}_{}".format(self._name, name),
                       **kwargs)
        self._workflow.add_nodes([node])
        return node

    def create_join_node(self, interface, joinfield, joinsource, name,
                         **kwargs):
        """
        Creates a JoinNode in the pipeline (prepending the pipeline
        namespace)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        joinfield : str | list(str)
            The name of the field(s) to join into a list
        joinsource : str
            Name of the "iterables" node over which to join
        name : str
            Name for the node
        requirements : list(Requirement)
            List of required packages need for the node to run (default: [])
        wall_time : float
            Time required to execute the node in minutes (default: 1)
        memory : int
            Required memory for the node in MB (default: 1000)
        nthreads : int
            Preferred number of threads to run the node on (default: 1)
        gpu : bool
            Flags whether a GPU compute node is preferred or not
            (default: False)
        account : str
            Name of the account to submit slurm scripts to
        """
        node = JoinNode(interface,
                        name="{}_{}".format(self._name, name),
                        joinsource=joinsource,
                        joinfield=joinfield, **kwargs)
        self._workflow.add_nodes([node])
        return node

    def create_join_visits_node(self, interface, joinfield, name, **kwargs):
        """
        Creates a JoinNode that joins an input over all visits for each subject
        (nipype.readthedocs.io/en/latest/users/joinnode_and_itersource.html)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        joinfield : str | list(str)
            The name of the field(s) to join into a list
        name : str
            Name for the node
        requirements : list(Requirement)
            List of required packages need for the node to run (default: [])
        wall_time : float
            Time required to execute the node in minutes (default: 1)
        memory : int
            Required memory for the node in MB (default: 1000)
        nthreads : int
            Preferred number of threads to run the node on (default: 1)
        gpu : bool
            Flags whether a GPU compute node is preferred or not
            (default: False)
        account : str
            Name of the account to submit slurm scripts to
        """
        node = JoinNode(interface,
                        joinsource='{}_sessions'.format(self.name),
                        joinfield=joinfield,
                        name="{}_{}".format(self._name, name), **kwargs)
        self._workflow.add_nodes([node])
        return node

    def create_join_subjects_node(self, interface, joinfield, name,
                                  **kwargs):
        """
        Creates a JoinNode that joins an input over all subjects for each visit
        (nipype.readthedocs.io/en/latest/users/joinnode_and_itersource.html)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        joinfield : str | list(str)
            The name of the field(s) to join into a list
        name : str
            Name for the node
        requirements : list(Requirement)
            List of required packages need for the node to run (default: [])
        wall_time : float
            Time required to execute the node in minutes (default: 1)
        memory : int
            Required memory for the node in MB (default: 1000)
        nthreads : int
            Preferred number of threads to run the node on (default: 1)
        gpu : bool
            Flags whether a GPU compute node is preferred or not
            (default: False)
        account : str
            Name of the account to submit slurm scripts to
        """
        node = JoinNode(interface,
                        joinsource='{}_subjects'.format(self.name),
                        joinfield=joinfield,
                        name='{}_{}'.format(self._name, name), **kwargs)
        self._workflow.add_nodes([node])
        return node

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

    def option(self, name):
        """
        Retrieves the value of the option provided to the pipeline's
        study and registers the option as being used by this pipeline
        for use in provenance capture

        Parameters
        ----------
        name : str
            The name of the option to retrieve
        """
        option = self.study._get_option(name)
        # Register option as being used by the pipeline
        self._used_options.add(option)
        return option.value

    @property
    def used_options(self):
        return iter(self._used_options)

    @property
    def all_options(self):
        """Return all options, including options of prerequisites"""
        return chain(self.options, self._prereq_options.iteritems())

    @property
    def non_default_options(self):
        return ((k, v) for k, v in self.options.iteritems()
                if v != self.default_options[k])

    @property
    def desc(self):
        return self._desc

    @property
    def inputnode(self):
        return self._inputnode

    def outputnode(self, frequency):
        """
        Returns the output node for the given frequency

        Parameters
        ----------
        frequency : str
            One of 'per_session', 'per_subject', 'per_visit' and
            'per_project', specifying whether the dataset is present for each
            session, subject, visit or project.
        """
        return self._outputnodes[frequency]

    @property
    def frequencies(self):
        "The frequencies present in the pipeline outputs"
        return self._outputs.iterkeys()

    def frequency_outputs(self, freq):
        return iter(self._outputs[freq])

    def frequency_output_names(self, freq):
        return (o.name for o in self.frequency_outputs(freq))

    def frequency(self, output):
        freqs = [m for m, outputs in self._outputs.itervalues()
                 if output in outputs]
        if not freqs:
            raise KeyError(
                "'{}' is not an output of pipeline '{}'".format(output,
                                                                self.name))
        else:
            assert len(freqs) == 1
            freq = freqs[0]
        return freq

    @property
    def citations(self):
        return self._citations

    def node(self, name):
        return self.workflow.get_node('{}_{}'.format(self.name, name))

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
        if input_name not in self.study.data_spec_names():
            raise ArcanaNameError(
                input_name,
                "'{}' is not a name of a specified dataset or field in {} "
                "Study".format(input_name, self.study.name))
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
