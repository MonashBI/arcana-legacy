from itertools import chain
from copy import copy, deepcopy
from logging import getLogger
from collections import defaultdict
from nipype.pipeline import engine as pe
from arcana.node import JoinNode
from arcana.interfaces.utils import Merge
from arcana.exception import (
    ArcanaError, ArcanaMissingDataException,
    ArcanaNoRunRequiredException, ArcanaNoConverterError,
    ArcanaUsageError)
from arcana.dataset.base import BaseDataset, BaseField
from arcana.interfaces.iterators import (
    InputSessions, PipelineReport, InputSubjects, SubjectReport,
    VisitReport, SubjectSessionReport, SessionReport)
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX

logger = getLogger('Arcana')


class BaseRunner(object):
    """
    A thin wrapper around the NiPype LinearPlugin used to connect
    runs pipelines on the local workstation

    Parameters
    ----------
    work_dir : str
        A directory in which to run the nipype workflows
    max_process_time : float
        The maximum time allowed for the process
    reprocess: True|False|'all'
        A flag which determines whether to rerun the processing for this
        step. If set to 'all' then pre-requisite pipelines will also be
        reprocessed.
    """

    default_plugin_args = {}

    def __init__(self, work_dir, max_process_time=None,
                 reprocess=False, **kwargs):
        self._work_dir = work_dir
        self._max_process_time = max_process_time
        self._reprocess = reprocess
        self._plugin_args = copy(self.default_plugin_args)
        self._plugin_args.update(kwargs)
        self._init_plugin()
        self._study = None

    def _init_plugin(self):
        self._plugin = self.nipype_plugin_cls(**self._plugin_args)

    @property
    def study(self):
        return self._study

    def bind(self, study):
        cpy = deepcopy(self)
        cpy._study = study
        return cpy

    def run(self, *pipelines, **kwargs):
        """
        Connects all pipelines to that study's archive and runs them
        in the same NiPype workflow

        Parameters
        ----------
        pipeline(s) : Pipeline, ...
            The pipeline to connect to archive
        subject_ids : List[str]
            The subset of subject IDs to process. If None all available will be
            reprocessed
        visit_ids: List[str]
            The subset of visit IDs for each subject to process. If None all
            available will be reprocessed

        Returns
        -------
        report : ReportNode
            The final report node, which can be connected to subsequent
            pipelines
        """
        if not pipelines:
            raise ArcanaUsageError(
                "No pipelines provided to {}.run"
                .format(self))
        # Create name by combining pipelines
        name = '_'.join(p.name for p in pipelines)
        workflow = pe.Workflow(name=name, base_dir=self.work_dir)
        already_connected = {}
        for pipeline in pipelines:
            try:
                self._connect_to_archive(
                    pipeline, workflow,
                    already_connected=already_connected, **kwargs)
            except ArcanaNoRunRequiredException:
                logger.info("Not running '{}' pipeline as its outputs "
                            "are already present in the archive"
                            .format(pipeline.name))
        # Reset the cached tree of datasets in the archive as it will
        # change after the pipeline has run.
        self.study.reset_tree()
        return workflow.run(plugin=self._plugin)

    def _connect_to_archive(self, pipeline, complete_workflow,
                            subject_ids=None, visit_ids=None,
                            already_connected=None):
        if already_connected is None:
            already_connected = {}
        try:
            (prev_connected, report) = already_connected[pipeline.name]
            if prev_connected == pipeline:
                return report
            else:
                raise ArcanaError(
                    "Name clash between {} and {} non-matching "
                    "prerequisite pipelines".format(prev_connected,
                                                    pipeline))
        except KeyError:
            pass
        # Check all inputs and outputs are connected
        pipeline.assert_connected()
        # Get list of sessions that need to be processed (i.e. if
        # they don't contain the outputs of this pipeline)
        sessions_to_process = self._sessions_to_process(
            pipeline, subject_ids=subject_ids, visit_ids=visit_ids)
        if not sessions_to_process:
            raise ArcanaNoRunRequiredException(
                "All outputs of '{}' are already present in project archive, "
                "skipping".format(pipeline.name))
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow.add_nodes([pipeline._workflow])
        # Get iterator nodes over subjects and sessions to be processed
        subjects, sessions = self._subject_and_session_iterators(
            pipeline, sessions_to_process, complete_workflow)
        # Prepend prerequisite pipelines to complete workflow if required
        if pipeline.has_prerequisites:
            reports = []
            prereq_subject_ids = list(
                set(s.subject.id for s in sessions_to_process))
            for prereq in pipeline.prerequisites:
                # NB: Even if reprocess==True, the prerequisite pipelines
                # are not re-processed, they are only reprocessed if
                # reprocess == 'all'
                try:
                    prereq_report = self._connect_to_archive(
                        prereq, complete_workflow=complete_workflow,
                        subject_ids=prereq_subject_ids,
                        visit_ids=visit_ids,
                        already_connected=already_connected)
                    reports.append(prereq_report)
                except ArcanaNoRunRequiredException:
                    logger.info(
                        "Not running '{}' pipeline as a "
                        "prerequisite of '{}' as the required "
                        "outputs are already present in the archive"
                        .format(prereq.name, pipeline.name))
            if reports:
                prereq_reports = pipeline.create_node(
                    Merge(len(reports)), 'prereq_reports')
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
            source = self.study.archive.source(
                (self.study.spec(i) for i in pipeline.inputs),
                study_name=self.study.name,
                name='{}_source'.format(pipeline.name))
        except ArcanaMissingDataException as e:
            raise ArcanaMissingDataException(
                str(e) + ", which is required for pipeline '{}'".format(
                    pipeline.name))
        # Map the subject and visit IDs to the input node of the pipeline
        # for use in connect_subject_id and connect_visit_id
        complete_workflow.connect(sessions, 'subject_id',
                                  pipeline.inputnode, 'subject_id')
        complete_workflow.connect(sessions, 'visit_id',
                                  pipeline.inputnode, 'visit_id')
        # Connect the nodes of the wrapper workflow
        complete_workflow.connect(sessions, 'subject_id',
                                  source, 'subject_id')
        complete_workflow.connect(sessions, 'visit_id',
                                  source, 'visit_id')
        for input_spec in pipeline.inputs:
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
                                        pipeline.name, self.study.name)))
                    conv_node_name = '{}_{}_input_conversion'.format(
                        pipeline.name, input_spec.name)
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
                                          pipeline.inputnode, input_spec.name)
            else:
                assert isinstance(input, BaseField)
                complete_workflow.connect(
                    source, input.name + FIELD_SUFFIX,
                    pipeline.inputnode, input_spec.name)
        # Create a report node for holding a summary of all the sessions/
        # subjects that were sunk. This is used to connect with dependent
        # pipelines into one large connected pipeline.
        report = pipeline.create_node(PipelineReport(), 'report')
        # Connect all outputs to the archive sink
        for freq, outputs in pipeline._outputs.iteritems():
            # Create a new sink for each frequency level (i.e 'per_session',
            # 'per_subject', 'per_visit', or 'per_project')
            sink = self.study.archive.sink(
                (self.study.spec(o) for o in outputs),
                frequency=freq,
                study_name=self.study.name,
                name='{}_{}_sink'.format(pipeline.name, freq))
#             sink.inputs.desc = pipeline.desc
#             sink.inputs.name = pipeline._study.name
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
                                            pipeline.name, self.study.name)))
                            conv_node_name = (output_spec.name +
                                              '_output_conversion')
                            (output_node, conv_in_field,
                             node_dataset_name) = converter.get_node(
                                 conv_node_name)
                            complete_workflow.connect(
                                pipeline._outputnodes[freq],
                                output_spec.name,
                                output_node, conv_in_field)
                        else:
                            output_node = pipeline._outputnodes[freq]
                            node_dataset_name = output.name
                        complete_workflow.connect(
                            output_node, node_dataset_name,
                            sink, output.name + PATH_SUFFIX)
                    else:
                        assert isinstance(output, BaseField)
                        complete_workflow.connect(
                            pipeline._outputnodes[freq], output.name, sink,
                            output.name + FIELD_SUFFIX)
            self._connect_to_reports(
                pipeline, sink, report, freq, subjects, sessions,
                complete_workflow)
        # Register pipeline as being connected to prevent duplicates
        already_connected[pipeline.name] = (pipeline, report)
        return report

    def _subject_and_session_iterators(self, pipeline,
                                       sessions_to_process, workflow):
        """
        Generate an input node that iterates over the sessions and subjects
        that need to be processed.
        """
        # Create nodes to control the iteration over subjects and sessions in
        # the project
        subjects = pipeline.create_node(
            InputSubjects(), 'subjects', wall_time=10, memory=1000)
        sessions = pipeline.create_node(
            InputSessions(), 'sessions', wall_time=10, memory=4000)
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
            sessions.itersource = ('{}_subjects'.format(pipeline.name),
                                   'subject_id')
            sessions.iterables = ('visit_id', subject_sessions)
        # Connect subject and session nodes together
        workflow.connect(subjects, 'subject_id', sessions, 'subject_id')
        return subjects, sessions

    def _connect_to_reports(self, pipeline, sink, output_summary, freq,
                            subjects, sessions, workflow):
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
                name=pipeline.name + '_session_outputs', wall_time=20,
                memory=4000)
            subject_session_outputs = JoinNode(
                SubjectSessionReport(), joinfield='subject_session_pairs',
                joinsource=subjects,
                name=pipeline.name + '_subject_session_outputs', wall_time=20,
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
                name=pipeline.name + '_subject_summary_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'subject_id',
                             subject_output_summary, 'subjects')
            workflow.connect(subject_output_summary, 'subjects',
                             output_summary, 'subjects')
        elif freq == 'per_visit':
            visit_output_summary = JoinNode(
                VisitReport(), joinsource=sessions, joinfield='sessions',
                name=pipeline.name + '_visit_summary_outputs', wall_time=20,
                memory=4000)
            workflow.connect(sink, 'visit_id',
                             visit_output_summary, 'sessions')
            workflow.connect(visit_output_summary, 'sessions',
                             output_summary, 'visits')
        elif freq == 'per_project':
            workflow.connect(sink, 'project_id', output_summary, 'project')

    def _sessions_to_process(self, pipeline, subject_ids=None,
                             visit_ids=None, reprocess=False):
        """
        Check whether the outputs of the pipeline are present in all sessions
        in the project archive, and make a list of the sessions and subjects
        that need to be reprocessed if they aren't.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to determine the sessions to process
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
        tree = self.study.tree
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
        for output_spec in pipeline.outputs:
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

    def __repr__(self):
        return "{}(work_dir={})".format(
            type(self).__name__, self._work_dir)

    def __eq__(self, other):
        try:
            return (self._work_dir == other._work_dir and
                    (self._max_process_time ==
                     other._max_process_time) and
                    self._plugin_args == other._plugin_args)
        except AttributeError:
            return False

    @property
    def work_dir(self):
        return self._work_dir

    def __getstate__(self):
        dct = copy(self.__dict__)
        # Delete the NiPype plugin as it can be regenerated
        del dct['_plugin']
        return dct

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init_plugin()
