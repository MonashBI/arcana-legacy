import os
import tempfile
import shutil
from itertools import chain
from collections import defaultdict
import subprocess as sp
from copy import copy
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface, Merge
from logging import getLogger
from nianalysis.exceptions import (
    NiAnalysisDatasetNameError, NiAnalysisError, NiAnalysisMissingDatasetError)
from nianalysis.data_formats import get_converter_node
from nianalysis.interfaces.utils import Chain
from nianalysis.interfaces.iterators import (
    InputSessions, PipelineReport, InputSubjects, SubjectReport,
    SubjectSessionReport, SessionReport)
from nianalysis.utils import INPUT_SUFFIX, OUTPUT_SUFFIX
from nianalysis.exceptions import NiAnalysisUsageError


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
        self._inputnode = self.create_node(
            IdentityInterface(fields=list(self.input_names)), "inputnode")
        # Set up outputs
        self._check_spec_names(outputs, 'output')
        self._outputs = defaultdict(list)
        for output in outputs:
            mult = self._study.dataset_spec(output).multiplicity
            self._outputs[mult].append(output)
        self._outputnodes = {}
        for mult in self._outputs:
            self._outputnodes[mult] = self.create_node(
                IdentityInterface(
                    fields=[o.name for o in self._outputs[mult]]),
                name="{}_outputnode".format(mult))
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

    @property
    def requires_gpu(self):
        return False  # FIXME: Need to implement this

    @property
    def max_memory(self):
        return 4000

    @property
    def wall_time(self):
        return '7-00:00:00'  # Max amount

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

    def run(self, work_dir=None, **kwargs):
        """
        Connects pipeline to archive and runs it on the local workstation

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
        self.connect_to_archive(complete_workflow, **kwargs)
        # Run the workflow
        return complete_workflow.run()

    def submit(self, scheduler='slurm', work_dir='/scratch/Monash016',
               cores=1, email=None, mail_on=('END', 'FAIL'),
               wall_time=None, **kwargs):
        """
        Submits a pipeline to a scheduler que for processing

        Parameters
        ----------
        scheduler : str
            Name of the scheduler to submit the pipeline to
        """
        if email is None:
            try:
                email = os.environ['EMAIL']
            except KeyError:
                raise NiAnalysisError(
                    "'email' needs to be provided if 'EMAIL' environment "
                    "variable not set")
        if scheduler == 'slurm':
            plugin = 'SLURMGraph'
            args = [('mail-user', email),
                    ('partition', 'm3c' if self.requires_gpu else 'm3d'),
                    ('ntasks', cores),
                    ('mem-per-cpu', self.max_memory),
                    ('cpus-per-task', 1),
                    ('time', (wall_time
                              if wall_time is not None else self.wall_time))]
            for mo in mail_on:
                args.append(('mail-type', mo))
            plugin_args = {
                'sbatch_args': ' '.join('--{}={}'.format(*a) for a in args)}
        else:
            raise NiAnalysisUsageError(
                "Unsupported scheduler '{}'".format(scheduler))
        complete_workflow = pe.Workflow(name=self.name, base_dir=work_dir)
        self.connect_to_archive(complete_workflow, **kwargs)
        return complete_workflow.run(plugin=plugin, plugin_args=plugin_args)

    def write_graph(self, fname, detailed=False, style='flat', complete=False):
        """
        Writes a graph of the pipeline to file

        Parameters
        ----------
        fname : str
            The filename for the saved graph
        detailed : bool
            Whether to save a detailed version of the graph or not
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
        if detailed:
            graph_file = 'graph_detailed.dot.png'
        else:
            graph_file = 'graph.dot.png'
        os.chdir(orig_dir)
        shutil.move(os.path.join(out_dir, graph_file), fname)
        shutil.rmtree(tmpdir)

    def connect_to_archive(self, complete_workflow, subject_ids=None,
                           filter_session_ids=None, reprocess=False,
                           project=None):
        """
        Gets a data source and data sink from the archive for the requested
        sessions, connects them to the pipeline's NiPyPE workflow

        Parameters
        ----------
        subject_ids : List[str]
            The subset of subject IDs to process. If None all available will be
            reprocessed
        filter_session_ids: List[str]
            The subset of session IDs for each subject to process. If None all
            available will be reprocessed
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

        Returns
        -------
        report : ReportNode
            The final report node to of the 
        """
        # Check all inputs and outputs are connected
        self.assert_connected()
        # Get list of available subjects and their associated sessions/datasets
        # from the archive
        if project is None:
            project = self._study.archive.project(
                self._study._project_id, subject_ids=subject_ids,
                session_ids=filter_session_ids)
        # Get list of sessions and subjects that need to be processed (i.e. if
        # they don't contain the outputs of this pipeline)
        sessions_to_process = self._sessions_to_process(
            project, filter_ids=filter_session_ids, reprocess=reprocess)
        if not sessions_to_process:
            logger.info(
                "All outputs of '{}' are already present in project archive, "
                "skipping".format(self.name))
            return None
        # Set up workflow to run the pipeline, loading and saving from the
        # archive
        complete_workflow.add_nodes([self._workflow])
        # Get iterator nodes over subjects and sessions to be processed
        subjects, sessions = self._subject_and_session_iterators(
            sessions_to_process, complete_workflow)
        # Prepend prerequisite pipelines to complete workflow if required
        prereqs = list(self.prerequisities)
        if prereqs:
            reports = []
            prereq_subject_ids = list(
                set(s.subject.id for s in sessions_to_process))
            for i, prereq in enumerate(prereqs, 1):
                # NB: Even if reprocess==True, the prerequisite pipelines are
                # not re-processed, they are only reprocessed if reprocess ==
                # 'all'
                prereq_report = prereq.connect_to_archive(
                    complete_workflow=complete_workflow,
                    subject_ids=prereq_subject_ids,
                    filter_session_ids=filter_session_ids,
                    reprocess=(reprocess if reprocess == 'all' else False),
                    project=project)
                if prereq_report is not None:
                    reports.append(prereq_report)
            if reports:
                prereq_reports = self.create_node(Merge(len(reports)),
                                                  'prereq_reports')
                for report in reports:
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
                self.study.project_id,
                (self.study.dataset(i) for i in self.inputs),
                study_name=self.study.name,
                name='{}_source'.format(self.name))
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
                conv_node_name = '{}_{}_input_conversion'.format(self.name,
                                                                  inpt.name)
                dataset_source, dataset_name = get_converter_node(
                    dataset, dataset.name + OUTPUT_SUFFIX, inpt.format,
                    source, complete_workflow, conv_node_name)
            else:
                dataset_source = source
                dataset_name = dataset.name + OUTPUT_SUFFIX
            # Connect the dataset to the pipeline input
            complete_workflow.connect(dataset_source, dataset_name,
                                      self.inputnode, inpt.name)
        # Create a report node for holding a summary of all the sessions/
        # subjects that were sunk. This is used to connect with dependent
        # pipelines into one large connected pipeline.
        report = self.create_node(PipelineReport(), 'report')
        # Connect all outputs to the archive sink
        for mult, outputs in self._outputs.iteritems():
            # Create a new sink for each multiplicity level (i.e 'per_session',
            # 'per_subject' or 'per_project')
            sink = self.study.archive.sink(
                self.study._project_id,
                (self.study.dataset(o) for o in outputs), mult,
                study_name=self.study.name,
                name='{}_{}_sink'.format(self.name, mult))
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
            self._connect_to_reports(
                sink, report, mult, subjects, sessions, complete_workflow)
        return report

    def _subject_and_session_iterators(self, sessions_to_process, workflow):
        """
        Generate an input node that iterates over the sessions and subjects
        that need to be processed.
        """
        # Create nodes to control the iteration over subjects and sessions in
        # the project
        subjects = self.create_node(InputSubjects(), 'subjects')
        sessions = self.create_node(InputSessions(), 'sessions')
        # Construct iterable over all subjects to process
        subjects_to_process = set(s.subject for s in sessions_to_process)
        subjects.iterables = ('subject_id',
                              tuple(s.id for s in subjects_to_process))
        # Determine whether the session ids are the same for every subject,
        # in which case they can be set as a constant, otherwise they will
        # need to be specified for each subject separately
        session_subjects = defaultdict(set)
        for session in sessions_to_process:
            session_subjects[session.id].add(session.subject.id)
        if all(ss == set(s.id for s in subjects_to_process)
               for ss in session_subjects.itervalues()):
            # All sessions are to be processed in every node, a simple second
            # layer of iterations on top of the subject iterations will
            # suffice. This allows re-combining on session_id across subjects
            sessions.iterables = ('session_id', session_subjects.keys())
        else:
            # Session IDs to be processed vary between subjects and so need
            # to be specified explicitly
            subject_sessions = defaultdict(list)
            for session in sessions_to_process:
                subject_sessions[session.subject.id].append(session.id)
            sessions.itersource = ('{}_subjects'.format(self.name),
                                   'subject_id')
            sessions.iterables = ('session_id', subject_sessions)
        # Connect subject and session nodes together
        workflow.connect(subjects, 'subject_id', sessions, 'subject_id')
        return subjects, sessions

    def _connect_to_reports(self, sink, output_summary, mult, subjects,
                            sessions, workflow):
        """
        Connects the sink of the pipeline to an "Output Summary", which lists
        the subjects and sessions that were processed for the pipeline. There
        should be only one summary node instance per pipeline so it can be
        used to feed into the input of subsequent pipelines to ensure that
        they are executed afterwards.
        """
        if mult == 'per_session':
            session_outputs = pe.JoinNode(
                SessionReport(), joinsource=sessions,
                joinfield=['subjects', 'sessions'],
                name=self.name + '_session_outputs')
            subject_session_outputs = pe.JoinNode(
                SubjectSessionReport(), joinfield='subject_session_pairs',
                joinsource=subjects,
                name=self.name + '_subject_session_outputs')
            workflow.connect(sink, 'subject_id', session_outputs, 'subjects')
            workflow.connect(sink, 'session_id', session_outputs, 'sessions')
            workflow.connect(session_outputs, 'subject_session_pairs',
                             subject_session_outputs, 'subject_session_pairs')
            workflow.connect(
                subject_session_outputs, 'subject_session_pairs',
                output_summary, 'subject_session_pairs')
        elif mult == 'per_subject':
            subject_output_summary = pe.JoinNode(
                SubjectReport(), joinsource=subjects, joinfield='subjects',
                name=self.name + '_subject_summary_outputs')
            workflow.connect(sink, 'subject_id',
                                      subject_output_summary, 'subjects')
            workflow.connect(subject_output_summary, 'subjects',
                                      output_summary, 'subjects')
        elif mult == 'per_project':
            workflow.connect(sink, 'project_id', output_summary, 'project')

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

    def _sessions_to_process(self, project, filter_ids=None, reprocess=False):
        """
        Check whether the outputs of the pipeline are present in all sessions
        in the project archive, and make a list of the sessions and subjects
        that need to be reprocessed if they aren't.

        Parameters
        ----------
        project : Project
            A representation of the project and associated subjects and
            sessions for the study's archive.
        filter_ids : list(str)
            Filter the session IDs to process
        """
        all_subjects = list(project.subjects)
        all_sessions = list(chain(*[s.sessions for s in all_subjects]))
        if reprocess:
            return all_sessions
        sessions_to_process = set()
        # Define filter function
        def filter_sessions(sessions):  # @IgnorePep8
            if filter_ids is None:
                return sessions
            else:
                return (s for s in sessions if s.id in filter_ids)
        for output in self.outputs:
            dataset = self.study.dataset(output)
            # If there is a project output then all subjects and sessions need
            # to be reprocessed
            if dataset.multiplicity == 'per_project':
                if dataset.prefixed_name not in project.dataset_names:
                    return all_sessions
            elif dataset.multiplicity == 'per_subject':
                sessions_to_process.update(chain(*(
                    filter_sessions(sub.sessions) for sub in all_subjects
                    if dataset.prefixed_name not in sub.dataset_names)))
            elif dataset.multiplicity == 'per_session':
                sessions_to_process.update(filter_sessions(
                    s for s in all_sessions
                    if dataset.prefixed_name not in s.dataset_names))
            else:
                assert False, "Unrecognised multiplicity of {}".format(dataset)
        return list(sessions_to_process)

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
                    # TODO: Need to implement Chain interface for
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

    def create_node(self, interface, name):
        """
        Creates a Node in the pipeline (prepending the pipeline namespace)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        name : str
            Name for the node
        """
        node = pe.Node(interface, name="{}_{}".format(self._name, name))
        self._workflow.add_nodes([node])
        return node

    def create_join_sessions_node(self, interface, joinfield, name):
        """
        Creates a JoinNode that joins an input over all sessions (see
        nipype.readthedocs.io/en/latest/users/joinnode_and_itersource.html)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        joinfield : str | list(str)
            The name of the field(s) to join into a list
        name : str
            Name for the node
        """
        node = pe.JoinNode(interface,
                           joinsource='{}_sessions'.format(self.name),
                           joinfield=joinfield, name=name)
        self._workflow.add_nodes([node])
        return node

    def create_join_subjects_node(self, interface, joinfield, name):
        """
        Creates a JoinNode that joins an input over all sessions (see
        nipype.readthedocs.io/en/latest/users/joinnode_and_itersource.html)

        Parameters
        ----------
        interface : nipype.Interface
            The interface to use for the node
        joinfield : str | list(str)
            The name of the field(s) to join into a list
        name : str
            Name for the node
        """
        node = pe.JoinNode(interface,
                           joinsource='{}_subjects'.format(self.name),
                           joinfield=joinfield, name=name)
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

    _slurm_jobscript_tmpl = """
    #!/bin/bash
# Usage: sbatch slurm-parallel-job-script
# Prepared By: Kai Xi,  Apr 2015
#              help@massive.org.au

# NOTE: To activate a SLURM option, remove the whitespace between the '#' and 'SBATCH'

# To give your job a name, replace "MyJob" with an appropriate name
# SBATCH --job-name=MyJob


# To set a project account for credit charging,
# SBATCH --account=pmosp


# Request CPU resource for a parallel job, for example:
#   4 Nodes each with 12 Cores/MPI processes
# SBATCH --ntasks=48
# SBATCH --ntasks-per-node=12
# SBATCH --cpus-per-task=1

# Memory usage (MB)
# SBATCH --mem-per-cpu=4000

# Set your minimum acceptable walltime, format: day-hours:minutes:seconds
# SBATCH --time=0-06:00:00


# To receive an email when job completes or fails
# SBATCH --mail-user=<You Email Address>
# SBATCH --mail-type=END
# SBATCH --mail-type=FAIL


# Set the file for output (stdout)
# SBATCH --output=MyJob-%j.out

# Set the file for error log (stderr)
# SBATCH --error=MyJob-%j.err


# Use reserved node to run job when a node reservation is made for you already
# SBATCH --reservation=reservation_name


# Command to run a MPI job,
#
# Option 1: 'srun' is a wrapper of 'mpirun' and it can automatically detect how many MPI processes to be launched
srun ./you_program

# Option 2: mpiexec
# For some cases, 'srun' does not perform the running behavior you want, you can still use raw MPI commands such as mpiexec, mpirun
mpiexec ./you_program


# If you want to enable bind-to-core option.
srun --cpu_bind=cores,v ./you_program
# Or
mpiexec --bind-to core --report-bindings ./you_program
"""
