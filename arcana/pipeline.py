import os
import tempfile
import shutil
from itertools import chain
from collections import defaultdict
from nipype.pipeline import engine as pe
import errno
from .node import Node, JoinNode, MapNode
from nipype.interfaces.utility import IdentityInterface
from logging import getLogger
from arcana.exception import (
    ArcanaNameError, ArcanaError, ArcanaOutputNotProducedException)


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
# 
#     def __hash__(self):
#         return (hash(self._name) ^
#                 hash(self._study) ^
#                 hash(self._desc) ^
#                 hash(self._version) ^
#                 hash(self._inputs) ^
#                 hash(self._outputs) ^
#                 hash(self._citations))

    def __ne__(self, other):
        return not (self == other)

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
        pipelines = set()
        required_outputs = defaultdict(set)
        for input in self.inputs:  # @ReservedAssignment
            spec = self._study.spec(input)
            # Could be an input to the study or optional acquired spec
            if spec.is_spec and spec.derived:
                pipelines.add(spec.pipeline)
                required_outputs[spec.pipeline_name].add(input.name)
        # Call pipeline-getter instance method on study with provided options
        # to generate pipeline to run
        for pipeline in pipelines:
            # Check that the required outputs are created with the given
            # options
            missing_outputs = required_outputs[pipeline.name] - set(
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

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

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

    def save_graph(self, fname, style='flat'):
        """
        Saves a graph of the pipeline to file

        Parameters
        ----------
        fname : str
            The filename for the saved graph
        style : str
            The style of the graph, can be one of can be one of
            'orig', 'flat', 'exec', 'hierarchical'
        plot : bool
            Whether to load and plot the graph after it has been written
        """
        fname = os.path.expanduser(fname)
        orig_dir = os.getcwd()
        tmpdir = tempfile.mkdtemp()
        os.chdir(tmpdir)
        workflow = self._workflow
        workflow.write_graph(graph2use=style)
        os.chdir(orig_dir)
        try:
            shutil.move(os.path.join(tmpdir, 'graph_detailed.png'),
                        fname)
        except IOError as e:
            if e.errno == errno.ENOENT:
                shutil.move(os.path.join(tmpdir, 'graph.png'), fname)
            else:
                raise
        shutil.rmtree(tmpdir)

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
