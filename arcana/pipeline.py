from builtins import str
from past.builtins import basestring
from builtins import object
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
from arcana.data.base import BaseFileset, BaseField, FilesetSpec
from arcana.exception import (
    ArcanaDesignError, ArcanaError, ArcanaOutputNotProducedException)


logger = getLogger('arcana')


class Pipeline(object):
    """
    A fairly thin wrapper around a NiPype workflow, which keeps track
    inputs and outputs, and maps names to avoid name-clashes and allow
    specialisation

    Parameters
    ----------
    study : Study
        The study from which the pipeline was created
    name : str
        The name of the pipeline
    desc : str
        A description of what the pipeline does
    references : List[Citation]
        List of scientific papers that describe the workflow and should be
        cited in publications that use it
    prefix : str
        Prefix prepended to the name of the pipeline and all node names within
        it. Typically passed in from a kwarg of a pipeline constructor in a
        sub-class or multi-study to create a duplicate of the pipeline
    input_map : str | dict[str,str] | function[str]
        Applied to the input names used by the pipeline to map them to new
        entries of the data specification in modified pipeline constructors.
        Often used in a sub-class or multi-study. If a string, the map is
        interpreted as a prefix applied to the names given in the original
        pipeline, if it is a dictionary the names are mapped explicitly, and
        if it is callable (e.g. a function) then it is expected to accept the
        name of the original input and return the new name.
    output_map : str | dict[str,str] | function[str]
        Same as the input map but applied to outputs instead of inputs to the
        pipeline.
    """
    SUBJECT_ITERFIELD = 'subject_id'
    VISIT_ITERFIELD = 'visit_id'
    ITERFIELDS = (SUBJECT_ITERFIELD, VISIT_ITERFIELD)

    def __init__(self, study, name, desc, references=None, prefix=None,
                 input_map=None, output_map=None):
        self._name = prefix + name
        self._input_map = input_map
        self._output_map = output_map
        self._study = study
        self._workflow = pe.Workflow(name=self.name)
        self._desc = desc
        self._input_conns = defaultdict(list)
        self._output_conns = {}
        # Set up inputs
#         self._check_spec_names(inputs, 'input')
#         if any(i.name in self.iterfields for i in inputs):
#             raise ArcanaError(
#                 "Cannot have a fileset spec named '{}' as it clashes with "
#                 "iterable field of that name".format(i.name))
#         self._inputs = inputs
#         self._inputnode = self.create_node(
#             IdentityInterface(fields=(
#                 tuple(self.input_names) + self.iterfields)),
#             name="inputnode", wall_time=10, memory=1000)
#         # Set up outputs
#         self._check_spec_names(outputs, 'output')
#         self._outputs = defaultdict(list)
#         for output in outputs:
#             freq = self._study.data_spec(output).frequency
#             self._outputs[freq].append(output)
#         for freq in self._outputs:
#             self._outputnodes[freq] = self.create_node(
#                 IdentityInterface(
#                     fields=[o.name for o in self._outputs[freq]]),
#                 name="{}_outputnode".format(freq), wall_time=10,
#                 memory=1000)
        self._references = references if references is not None else []
        # For recording which parameters are accessed
        # during pipeline generation so they can be attributed to the
        # pipeline after it is generated (and then saved in the
        # provenance
        self._referenced_parameters = None

    def __repr__(self):
        return "{}(name='{}')".format(self.__class__.__name__,
                                      self.name)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._name == other._name and
            self._desc == other._desc and
            self._input_conns == other._input_conns and
            self._output_conns == other._output_conns and
            self._references == other._references)

    def __hash__(self):
        return (hash(self._name) ^
                hash(self._desc) ^
                hash(tuple(self._input_conns)) ^
                hash(tuple(self._output_conns)) ^
                hash(tuple(self._references)))

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
        # Call pipeline-getter instance method on study with provided
        # parameters to generate pipeline to run
        for pipeline in pipelines:
            # Check that the required outputs are created with the given
            # parameters
            missing_outputs = required_outputs[pipeline.name] - set(
                d.name for d in pipeline.outputs)
            if missing_outputs:
                raise ArcanaOutputNotProducedException(
                    "Output(s) '{}', required for '{}' pipeline, will "
                    "not be created by prerequisite pipeline '{}' "
                    "with parameters: {}".format(
                        "', '".join(missing_outputs), self.name,
                        pipeline.name,
                        '\n'.join('{}={}'.format(o.name, o.value)
                                  for o in self.study.parameters)))
            yield pipeline

    @property
    def study_inputs(self):
        """
        Returns all inputs to the pipeline, including inputs of
        prerequisites (and their prerequisites recursively)
        """
        return chain((i for i in self.inputs if not i.derived),
                     *(p.study_inputs for p in self.prerequisites))

    def add(self, name, interface, node_type=None, **kwargs):
        """
        Adds a processing Node to the pipeline

        Parameters
        ----------
        name : str
            Name for the node
        interface : nipype.Interface
            The interface to use for the node
        node_type : str | None
            The type of node to create. Can be one of 'map', 'join',
            'join_subjects', 'join_visits', or None. If 'map' a MapNode is
            used, if starts with 'join' a JoinNode is used. The special join
            nodes 'join_subjects' and 'join_visits' join on the iterator nodes
            for subjects and visits, respectively. If None then a regular Node
            is used.
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

        Returns
        -------
        node : Node
            The Node object that has been added to the pipeline
        """
        if node_type == 'map':
            node_cls = MapNode
        elif node_type in ('join', 'join_subjects', 'join_visits'):
            node_cls = JoinNode
        else:
            node_cls = Node
        if node_type == 'join_subjects':
            kwargs['joinsource'] = '{}_subjects'.format(self.name)
        elif node_type == 'join_visits':
            kwargs['joinsource'] = '{}_sessions'.format(self.name)
        node = node_cls(interface, name="{}_{}".format(self._name, name),
                        processor=self.study.processor, **kwargs)
        self._workflow.add_nodes([node])
        return node

    def connect_input(self, spec_name, node, node_input, format=None, **kwargs):  # @ReservedAssignment @IgnorePep8
        """
        Connects a study fileset_spec as an input to the provided node

        Parameters
        ----------
        spec_name : str
            Name of the study data spec (or one of the IDs from the iterator
            nodes, 'subject_id' or 'visit_id') to connect to the node
        node : arcana.Node
            The node to connect the input to
        node_input : str
            Name of the input on the node to connect the fileset spec to
        format : FileFormat | None
            The file format the input is expected in. If it differs
            from the format in data spec or of study input then an implicit
            conversion is performed. If None the file format in the data spec
            is assumed
        """
        name = self._map_name(spec_name, self._input_map)
        if name not in self.study.data_spec_names():
            raise ArcanaDesignError(
                "Proposed input '{}' to '{}' pipeline is not a valid spec "
                "name for {} studies ('{}')"
                .format(name, self.name, self.study.__class__.__name__,
                        "', '".join(self.study.data_spec_names())))
        self._input_conns[name].append((node, node_input, format, kwargs))

    def connect_output(self, spec_name, node, node_output, format=None, **kwargs):  # @ReservedAssignment @IgnorePep8
        """
        Connects an output to a study fileset spec

        Parameters
        ----------
        spec_name : str
            Name of the study fileset spec to connect to
        node : arcana.Node
            The node to connect the output from
        node_output : str
            Name of the output on the node to connect to the fileset
        format : FileFormat | None
            The file format the output is returned in. If it differs
            from the format in data spec then an implicit conversion is
            performed. If None the it is assumed to be returned in the file
            format of the entry the data spec
        """
        name = self._map_name(spec_name, self._output_map)
        if name not in self.study.data_spec_names():
            raise ArcanaDesignError(
                "Proposed output '{}' to '{}' pipeline is not a valid spec "
                "name for {} studies ('{}')"
                .format(name, self.study.__class__.__name__,
                        "', '".join(self.study.data_spec_names())))
        if name in self._output_conns:
            raise ArcanaDesignError(
                "'{}' output of '{} pipeline of {} study has already been "
                "connected".format(name, self.name,
                                   self.study.__class__.__name__))
        self._output_conns[name] = (node, node_output, format, kwargs)

    def _map_name(self, name, mapper):
        """
        Maps a spec name to a new value based on the provided mapper
        """
        if isinstance(mapper, basestring):
            mapped = mapper + '_' + name
        elif isinstance(mapper, dict):
            mapped = mapper[name]
        else:
            mapped = mapper(name)
        return mapped

    def connect(self, *args, **kwargs):
        """
        Performs the connection in the wrapped NiPype workflow
        """
        self._workflow.connect(*args, **kwargs)

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
    def inputs(self):
        return (self.study.input(i) for i in self._input_conns)

    @property
    def outputs(self):
        return (self.study.data_spec(o) for o in self._output_conns)

    @property
    def input_names(self):
        return self._input_conns.keys()

    @property
    def output_names(self):
        return self._output_conns.keys()

    @property
    def input_frequencies(self):
        return set(i.frequency for i in self.inputs)

    @property
    def output_frequencies(self):
        return set(o.frequency for o in self.ouputs)

    def frequency_inputs(self, frequency):
        return (i for i in self._input_conns if i.frequency == frequency)

    def frequency_outputs(self, frequency):
        return (o for o in self._output_conns if o.frequency == frequency)

    @property
    def referenced_parameters(self):
        return iter(self._referenced_parameters)

    @property
    def all_parameters(self):
        """Return all parameters, including parameters of prerequisites"""
        return chain(self.parameters, iter(self._prereq_parameters.items()))

    @property
    def non_default_parameters(self):
        return ((k, v) for k, v in self.parameters.items()
                if v != self.default_parameters[k])

    @property
    def desc(self):
        return self._desc

    def inputnode(self, frequency):
        """
        Returns the input node for the given frequency, generating if it
        hasn't been already. It also adds implicit file format conversion nodes
        to the pipeline.

        Parameters
        ----------
        frequency : str
            The frequency (i.e. 'per_session', 'per_visit', 'per_subject' or
            'per_study') of the input node to retrieve
        """
        # Check to see whether there are any outputs for the given frequency
        inputs = self.frequency_inputs(frequency)
        if not inputs:
            raise ArcanaError(
                "No inputs to '{}' pipeline for requested freqency '{}'"
                .format(self.name, frequency))
        # Get list of input names for the requested frequency, addding fields
        # to hold iterator IDs
        input_names = [i.name for i in inputs]
        for iterfield in self.ITERFIELDS:
            if self.iterates_over(frequency, iterfield):
                input_names.append(iterfield)
        try:
            # Check to see whether the input node has already been created
            inputnode = self._inputnodes[frequency]
            if sorted(inputnode.inputs) != sorted(input_names):
                raise ArcanaError(
                    "Pipeline '{}' has been altered since its input node was "
                    "created.".format(self.name))
        except KeyError:
            # Generate input node and connect it to appropriate nodes
            inputnode = self._inputnodes[frequency] = self.add(
                '{}_inputnode'.format(frequency), IdentityInterface(
                    fields=input_names))
            # Loop through list of nodes connected to study data specs and
            # connect them to the newly created input node
            for input in inputs:  # @ReservedAssignment
                conv_cache = {}
                for (node, node_in,
                     format, conv_kwargs) in self._input_conns[input.name]:  # @ReservedAssignment @IgnorePep8
                    # If fileset formats differ between study and pipeline
                    # inputs create converter node (if one hasn't been already)
                    # and connect input to that before connecting to inputnode
                    if isinstance(input, BaseFileset) and (format !=
                                                           input.format):
                        if format.name not in conv_cache:
                            conv_cache[format.name] = format.converter_from(
                                input.format, **conv_kwargs)
                        (conv_node,
                         conv_in, conv_out) = conv_cache[format.name].get_node(
                            '{}_{}_{}_to_{}_conversion'.format(
                                self.name, input.name, input.format.name,
                                format.name))
                        self.connect(inputnode, input.name, conv_node, conv_in)
                        self.connect(conv_node, conv_out, node, node_in)
                    else:
                        self.connect(inputnode, input.name, node, node_in)
        return inputnode

    def outputnode(self, frequency):
        """
        Returns the output node for the given frequency, generating if it
        hasn't been already

        Parameters
        ----------
        frequency : str
            The frequency (i.e. 'per_session', 'per_visit', 'per_subject' or
            'per_study') of the output node to retrieve
        """
        # Check to see whether there are any outputs for the given frequency
        outputs = self.frequency_outputs(frequency)
        if not outputs:
            raise ArcanaError(
                "No outputs to '{}' pipeline for requested freqency '{}'"
                .format(self.name, frequency))
        # Get list of output names for the requested frequency, addding fields
        # to hold iterator IDs
        output_names = [i.name for i in outputs]
        for iterfield in self.ITERFIELDS:
            if self.iterates_over(frequency, iterfield):
                output_names.append(iterfield)
        try:
            # Check to see whether the output node has already been created
            outputnode = self._outputnodes[frequency]
            if sorted(outputnode.outputs) != sorted(output_names):
                raise ArcanaError(
                    "Pipeline '{}' has been altered since its output node was "
                    "created.".format(self.name))
        except KeyError:
            # Generate output node and connect it to appropriate nodes
            outputnode = self._outputnodes[frequency] = self.add(
                '{}_outputnode'.format(frequency), IdentityInterface(
                    fields=output_names))
            # Loop through list of nodes connected to study data specs and
            # connect them to the newly created output node
            for output in outputs:  # @ReservedAssignment
                conv_cache = {}
                (node, node_out,
                 format, conv_kwargs) = self._output_conns[output.name]  # @ReservedAssignment @IgnorePep8
                # If fileset formats differ between study and pipeline
                # outputs create converter node (if one hasn't been already)
                # and connect output to that before connecting to outputnode
                if isinstance(output, BaseFileset) and (format !=
                                                        output.format):
                    if format.name not in conv_cache:
                        conv_cache[format.name] = output.format.converter_from(
                            format, **conv_kwargs)
                    (conv_node,
                     conv_in, conv_out) = conv_cache[format.name].get_node(
                        '{}_{}_{}_to_{}_conversion'.format(
                            self.name, output.name, output.format.name,
                            format.name))
                self.connect(node, node_out, conv_node, conv_in)
                self.connect(conv_node, conv_out, outputnode, output.name)
            else:
                self.connect(node, node_out, outputnode, output.name)
        return outputnode


#     @property
#     def frequencies(self):
#         "The frequencies present in the pipeline outputs"
#         return iter(self._outputs.keys())
# 
#     def frequency_outputs(self, freq):
#         return iter(self._outputs[freq])
# 
#     def frequency_output_names(self, freq):
#         return (o.name for o in self.frequency_outputs(freq))
# 
#     def frequency(self, output):
#         freqs = [m for m, outputs in self._outputs.values()
#                  if output in outputs]
#         if not freqs:
#             raise KeyError(
#                 "'{}' is not an output of pipeline '{}'".format(output,
#                                                                 self.name))
#         else:
#             assert len(freqs) == 1
#             freq = freqs[0]
#         return freq

    @property
    def references(self):
        return self._references

    def node(self, name):
        return self.workflow.get_node('{}_{}'.format(self.name, name))

    @property
    def suffix(self):
        """
        A suffixed appended to output filenames when they are repositoryd to
        identify the parameters used to generate them
        """
        return '__'.join('{}_{}'.format(k, v)
                         for k, v in self.parameters.items())

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
        if not fname.endswith('.png'):
            fname += '.png'
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

    @classmethod
    def iterates_over(cls, frequency, iterfield):  # @ReservedAssignment
        if iterfield == cls.SUBJECT_ITERFIELD:
            iterates = (frequency in ('per_session', 'per_subject'))
        elif iterfield == cls.VISIT_ITERFIELD:
            iterates = (frequency in ('per_session', 'per_visit'))
        else:
            raise ArcanaError("Unrecognised iterfield '{}', can be one of '{}'"
                              .format(iterfield, "', '".join(cls.ITERFIELDS)))
        return iterates
