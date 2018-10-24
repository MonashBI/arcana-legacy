from past.builtins import basestring
from builtins import object
import os
from copy import deepcopy
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
    ArcanaDesignError, ArcanaNameError, ArcanaError,
    ArcanaOutputNotProducedException, ArcanaMissingDataException)


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
    name_maps : dict
        A dictionary containing several modifying keyword arguments that
        manipulate way the pipeline is constructed (e.g. map inputs and outputs
        to new entries in the data specification table). Typically names of
        inputs, outputs and the pipeline itself. Intended to allow secondary
        pipeline constructors to call a constructor, and return a modified
        version of the pipeline it returns.

        It should be passed directly from wildcard keyword args passed to the
        pipeline constructor, e.g.

        def my_pipeline(**name_maps):
            pipeline = self.pipeline('my_pipeline', mods)
            pipeline.add('a_node', MyInterface())

            ...

            return pipeline

        The keywords in 'name_mods' used in pipeline construction are:

        name : str
            A new name for the pipeline
        prefix : str
            Prefix prepended to the original name of the pipeline. Typically
            only one of name and prefix is used at each nested level, but they
            can be used in conjunction.
        input_map : str | dict[str,str]
            Applied to the input names used by the pipeline to map them to new
            entries of the data specification in modified pipeline
            constructors. Typically used in sub-class or multi-study. If a
            string, the map is interpreted as a prefix applied to the names
            given in the original pipeline, if it is a dictionary the names are
            mapped explicitly.
        output_map : str | dict[str,str]
            Same as the input map but applied to outputs instead of inputs to
            the pipeline.
        name_maps : dict
            Modifications from nested pipeline constructors
        study : Study
            A different study to bind the pipeline to from the one containing
            the inner pipeline constructor. Intended to be used with
            multi-studies.
    desc : str
        A description of what the pipeline does
    references : List[Citation]
        List of scientific papers that describe the workflow and should be
        cited in publications that use it
    """

    def __init__(self, study, name, name_maps, desc=None, references=None):
        name, study, maps = self._unwrap_mods(name_maps, name, study=study)
        self._name = name
        self._input_map = maps.get('input_map', None)
        self._output_map = maps.get('output_map', None)
        self._study = study
        self._workflow = pe.Workflow(name=self.name)
        self._desc = desc
        self._input_conns = defaultdict(list)
        self._iterator_conns = defaultdict(list)
        self._output_conns = {}
        self._iterator_joins = set()
        # Set up inputs
        self._references = references if references is not None else []
        # For recording which parameters are accessed
        # during pipeline generation so they can be attributed to the
        # pipeline after it is generated (and then saved in the
        # provenance
        self._referenced_parameters = None
        self._required_outputs = set()

    def __repr__(self):
        return "{}(name='{}')".format(self.__class__.__name__,
                                      self.name)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._name == other._name and
            self._desc == other._desc and
            set(self._input_conns.keys()) == set(other._input_conns.keys()) and
            (set(self._output_conns.keys()) ==
             set(other._output_conns.keys())) and
            self._references == other._references)

    def __hash__(self):
        return (hash(self._name) ^
                hash(self._desc) ^
                hash(tuple(self._input_conns.keys())) ^
                hash(tuple(self._output_conns.keys())) ^
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
        pipelines = {}
        for input in self.inputs:  # @ReservedAssignment
            try:
                spec = self._study.spec(input)
            except ArcanaMissingDataException as e:
                raise ArcanaMissingDataException(
                    "{}, which is required as an input of the '{}' pipeline to"
                    " produce '{}'"
                    .format(e, self.name, "', '".join(self.required_outputs)))
            # Could be an input to the study or optional acquired spec
            if spec.is_spec and spec.derived:
                try:
                    pipeline = pipelines[spec.pipeline_name]
                except KeyError:
                    pipeline = pipelines[spec.pipeline_name] = spec.pipeline
                pipeline._required_outputs.add(input.name)
        # Call pipeline-getter instance method on study with provided
        # parameters to generate pipeline to run
        for pipeline in pipelines.values():
            # Check that the required outputs are created with the given
            # parameters
            missing_outputs = pipeline._required_outputs - set(
                d.name for d in pipeline.outputs)
            if missing_outputs:
                raise ArcanaOutputNotProducedException(
                    "Output(s) '{}', required for {}, will "
                    "not be created by prerequisite pipeline '{}' "
                    "with parameters: {}".format(
                        "', '".join(missing_outputs), self._error_msg_loc,
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

    def add(self, name, interface, inputs=None, outputs=None, connect=None,
            **kwargs):
        """
        Adds a processing Node to the pipeline

        Parameters
        ----------
        name : str
            Name for the node
        interface : nipype.Interface
            The interface to use for the node
        inputs : dict[str, str | (str, FileFormat)]
            Connections from inputs of the pipeline to inputs of the interface.
            The keys of the dictionary are the field names and the values
            are the the name of the data spec or a tuple containing the name
            of the spec and the data format it is expected in. Note that input
            connections can also be specified using the 'connect_input'
            method.
        outputs : dict[str, str | (str, FileFormat)]
            Connections to outputs of the pipeline from fields of the
            interface. The keys of the dictionary are the field names and the
            values are the the name of the data spec or a tuple containing the
            name of the spec and the data format it is produced in. Note that
            output connections can also be specified using the 'connect_output'
            method.
        connect : dict[str, (Node, str)]
            A dictionary containing connections from other nodes within the
            pipeline to inputs of the interface. The values of the dictionary
            are 2-tuples with the first item the sending Node and the second
            the name of an output of the sending Node. Note that inputs can
            also be specified outside this method using the 'connect' method.
        iterfield : str
            Name of field to be passed an iterable to iterator over.
            If present, a MapNode will be created instead of a regular node
        joinsource : str
            Name of iterator field to join. Typically one of the implicit
            iterators (i.e. Study.SUBJECT_ID or Study.VISIT_ID)
            to join over the subjects and/or visits
        joinfield : str
            Name of field to pass the joined list when creating a JoinNode
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
        if 'iterfield' in kwargs:
            if 'joinfield' in kwargs or 'joinsource' in kwargs:
                raise ArcanaDesignError(
                    "Cannot provide both joinsource and iterfield to when "
                    "attempting to add '{}' node to {}"
                    .foramt(name, self._error_msg_loc))
            node_cls = MapNode
        elif 'joinsource' in kwargs or 'joinfield' in kwargs:
            if not ('joinfield' in kwargs and 'joinsource' in kwargs):
                raise ArcanaDesignError(
                    "Both joinsource and joinfield kwargs are required to "
                    "create a JoinNode (see {})".format(name,
                                                        self._error_msg_loc))
            joinsource = kwargs['joinsource']
            if joinsource in self.study.ITERFIELDS:
                self._iterator_joins.add(joinsource)
            node_cls = JoinNode
            # Prepend name of pipeline of joinsource to match name of nodes
            kwargs['joinsource'] = '{}_{}'.format(self.name, joinsource)
        else:
            node_cls = Node
        node = node_cls(interface, name="{}_{}".format(self._name, name),
                        environment=self.study.environment, **kwargs)
        # Ensure node is added to workflow
        self._workflow.add_nodes([node])
        # Connect inputs, outputs and internal connections
        if inputs is not None:
            assert isinstance(inputs, dict)
            for node_input, (input, input_format) in inputs.items():  # @ReservedAssignment @IgnorePep8
                self.connect_input(input, node, node_input, input_format)
        if outputs is not None:
            assert isinstance(outputs, dict)
            for node_input, (output, output_format) in outputs.items():
                self.connect_output(output, node, node_input, output_format)
        # Connect internal noes of the pipeline
        if connect is not None:
            assert isinstance(connect, dict)
            for node_input, (conn_node, conn_field) in connect.items():
                self.connect(conn_node, conn_field, node, node_input)
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
        if spec_name in self.study.ITERFIELDS:
            self._iterator_conns[spec_name].append((node, node_input, format))
        else:
            name = self._map_name(spec_name, self._input_map)
            if name not in self.study.data_spec_names():
                raise ArcanaDesignError(
                    "Proposed input '{}' to {} is not a valid spec name ('{}')"
                    .format(name, self._error_msg_loc,
                            "', '".join(self.study.data_spec_names())))
            self._input_conns[name].append((node, node_input, format, kwargs))

    def connect_output(self, spec_name, node, node_output, format=None,   # @ReservedAssignment @IgnorePep8
                       **kwargs):
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
                "Proposed output '{}' to {} is not a valid spec name ('{}')"
                .format(name, self._error_msg_loc,
                        "', '".join(self.study.data_spec_names())))
        if name in self._output_conns:
            raise ArcanaDesignError(
                "'{}' output of {} has already been connected"
                .format(name, self._error_msg_loc))
        self._output_conns[name] = (node, node_output, format, kwargs)

    def _map_name(self, name, mapper):
        """
        Maps a spec name to a new value based on the provided mapper
        """
        if mapper is not None:
            if isinstance(mapper, basestring):
                name = mapper + name
            try:
                name = mapper[name]
            except KeyError:
                pass
        return name

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
    def references(self):
        return self._references

    @property
    def inputs(self):
        for inpt in self._input_conns:
            try:
                yield self.study.input(inpt)
            except ArcanaNameError:
                yield self.study.data_spec(inpt)

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
    def joins_subjects(self):
        "Iterators that are joined within the pipeline"
        return self.study.SUBJECT_ID in self._iterator_joins

    @property
    def joins_visits(self):
        "Iterators that are joined within the pipeline"
        return self.study.VISIT_ID in self._iterator_joins

    @property
    def input_frequencies(self):
        freqs = set(i.frequency for i in self.inputs)
        if self.study.SUBJECT_ID in self._iterator_conns:
            freqs.add('per_subject')
        if self.study.VISIT_ID in self._iterator_conns:
            freqs.add('per_visit')
        return freqs

    @property
    def output_frequencies(self):
        return set(o.frequency for o in self.outputs)

    def frequency_inputs(self, frequency):
        return (i for i in self.inputs if i.frequency == frequency)

    def frequency_outputs(self, frequency):
        return (o for o in self.outputs if o.frequency == frequency)

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
    def required_outputs(self):
        return self._required_outputs

    @property
    def desc(self):
        return self._desc

    def inputnode(self, frequency):
        """
        Generates an input node for the given frequency. It also adds implicit
        file format conversion nodes to the pipeline.

        Parameters
        ----------
        frequency : str
            The frequency (i.e. 'per_session', 'per_visit', 'per_subject' or
            'per_study') of the input node to retrieve
        """
        # Check to see whether there are any outputs for the given frequency
        inputs = list(self.frequency_inputs(frequency))
        # Get list of input names for the requested frequency, addding fields
        # to hold iterator IDs
        input_names = [i.name for i in inputs]
        input_names.extend(self.study.FREQUENCIES[frequency])
#         if frequency == 'per_subject':
#             input_names.append(self.study.SUBJECT_ID)
#         elif frequency == 'per_visit':
#             input_names.append(self.study.VISIT_ID)
#         for iterfield in self.study.ITERFIELDS:
#             if self.iterates_over(iterfield, frequency):
#                 input_names.append(iterfield)
        if not input_names:
            raise ArcanaError(
                "No inputs to '{}' pipeline for requested freqency '{}'"
                .format(self.name, frequency))
        # Generate input node and connect it to appropriate nodes
        inputnode = self.add('{}_inputnode'.format(frequency),
                             IdentityInterface(fields=input_names))
        # Loop through list of nodes connected to study data specs and
        # connect them to the newly created input node
        for input in inputs:  # @ReservedAssignment
            conv_cache = {}
            for (node, node_in,
                 format, conv_kwargs) in self._input_conns[input.name]:  # @ReservedAssignment @IgnorePep8
                # If fileset formats differ between study and pipeline
                # inputs create converter node (if one hasn't been already)
                # and connect input to that before connecting to inputnode
                if self.requires_conversion(input, format):
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
        # Connect iterator inputs
        for iterfield, conns in self._iterator_conns.items():
            # Check to see if this is the right frequency for the iterator
            # input, i.e. if it is the only iterfield for this frequency
            if self.study.FREQUENCIES[frequency] == (iterfield,):
                for (node, node_in, format) in conns:  # @ReservedAssignment
                    self.connect(inputnode, iterfield, node, node_in)
        return inputnode

    def outputnode(self, frequency):
        """
        Generates an output node for the given frequency. It also adds implicit
        file format conversion nodes to the pipeline.

        Parameters
        ----------
        frequency : str
            The frequency (i.e. 'per_session', 'per_visit', 'per_subject' or
            'per_study') of the output node to retrieve
        """
        # Check to see whether there are any outputs for the given frequency
        outputs = list(self.frequency_outputs(frequency))
        if not outputs:
            raise ArcanaError(
                "No outputs to '{}' pipeline for requested freqency '{}'"
                .format(self.name, frequency))
        # Get list of output names for the requested frequency, addding fields
        # to hold iterator IDs
        output_names = [o.name for o in outputs]
        # Generate output node and connect it to appropriate nodes
        outputnode = self.add('{}_outputnode'.format(frequency),
                              IdentityInterface(fields=output_names))
        # Loop through list of nodes connected to study data specs and
        # connect them to the newly created output node
        for output in outputs:  # @ReservedAssignment
            conv_cache = {}
            (node, node_out,
             format, conv_kwargs) = self._output_conns[output.name]  # @ReservedAssignment @IgnorePep8
            # If fileset formats differ between study and pipeline
            # outputs create converter node (if one hasn't been already)
            # and connect output to that before connecting to outputnode
            if self.requires_conversion(output, format):
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

    @classmethod
    def requires_conversion(cls, fileset, file_format):
        """Checks whether the fileset matches the requested file format"""
        if file_format is None:
            return False
        try:
            filset_format = fileset.format
        except AttributeError:
            return False  # Field input
        else:
            return (file_format != filset_format)

    def node(self, name):
        return self.workflow.get_node('{}_{}'.format(self.name, name))

    def save_graph(self, fname, style='flat', format='png', **kwargs):
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
        workflow.write_graph(graph2use=style, format=format, **kwargs)
        os.chdir(orig_dir)
        try:
            shutil.move(os.path.join(tmpdir, 'graph_detailed.{}'
                                     .format(format)), fname)
        except IOError as e:
            if e.errno == errno.ENOENT:
                shutil.move(os.path.join(tmpdir, 'graph.{}'.format(format)),
                            fname)
            else:
                raise
        shutil.rmtree(tmpdir)

    def iterfields(self, frequency=None):
        """
        Returns the iterfields (i.e. subject_id, visit_id) that the pipeline
        iterates over

        Parameters
        ----------
        frequency : str | None
            A selected data frequency to use to determine which iterfields are
            required. If None, all input frequencies of the pipeline are
            assumed
        """
        iterfields = set()
        if frequency is None:
            input_freqs = list(self.input_frequencies)
        else:
            input_freqs = [frequency]
        for freq in input_freqs:
            iterfields.update(self.study.FREQUENCIES[freq])
        return iterfields

    def iterates_over(self, iterfield, freq):
        """
        Checks to see if the given frequency requires iteration over the
        given iterfield

        Parameters
        ----------
        iterfield : str
            The iterfield to check
        freq : str
            The frequency to check
        """
        return iterfield in self.study.FREQUENCIES[freq]

    def metadata_mismatch(self, item):
        """
        Determines whether the given derivative dataset needs to be
        (re)processed or not, checking the item's provenance against the
        parameters used by the pipeline.

        Parameters
        ----------
        item : Fileset | Field
            The item to check for reprocessing

        Returns
        -------
        to_process : bool
            Whether to (re)process the given item
        """
        # TODO: Add provenance checking for items that exist
        return not item.exists

    def _unwrap_mods(self, name_maps, name, study=None, **inner_maps):
        """
        Unwraps potentially nested modification dictionaries to get values
        for name, input_map, output_map and study. Unsed in __init__.

        Parameters
        ----------
        name_maps : dict
            A dictionary containing the name_maps to apply to the values
        name : str
            Name passed from inner pipeline constructor
        study : Study
            The study to bind the pipeline to. Will be overridden by any values
            in the mods dict
        inner_maps : dict[str, dict[str,str]]
            input and output maps from inner pipeline constructors

        Returns
        -------
        name : str
            Potentially modified name of the pipeline
        study : Study
            Potentially modified study
        maps : dict[str, dict[str,str]]
            Potentially modifed input and output maps
        """
        # Set values of name and study
        name = name_maps.get('name', name)
        name = name_maps.get('prefix', '') + name
        study = name_maps.get('study', study)
        # Flatten input and output maps, combining maps from inner nests with
        # those in the "mods" dictionary
        maps = {}
        for mtype in ('input_map', 'output_map'):
            try:
                inner_map = inner_maps[mtype]
            except KeyError:
                try:
                    maps[mtype] = name_maps[mtype]  # Only outer map
                except KeyError:
                    pass  # No maps
            else:
                try:
                    outer_map = name_maps[mtype]
                except KeyError:
                    maps[mtype] = inner_map  # Only inner map
                else:
                    # Work through different combinations of  inner and outer
                    # map types (i.e. str & str, str & dict, dict & str, and
                    # dict & dict) and combine into a single map
                    if isinstance(outer_map, basestring):
                        if isinstance(inner_map, basestring):
                            # Concatenate prefixes
                            maps[mtype] = outer_map + inner_map
                        elif isinstance(inner_map, dict):
                            # Add outer_map prefix to all values in inner map
                            # dictionary
                            maps[mtype] = {k: outer_map + v
                                           for k, v in inner_map.items()}
                        else:
                            raise ArcanaDesignError(
                                "Unrecognised type for name map in '{}' "
                                "pipeline can be str or dict[str,str]: {}"
                                .format(name, inner_map))
                    elif isinstance(outer_map, dict):
                        if isinstance(inner_map, basestring):
                            # Strip inner map prefix from outer dictionary
                            # (which should have prefix included). This should
                            # be an unlikely case I imagine
                            maps[mtype] = {k[len(inner_map):]: v
                                           for k, v in outer_map.items()}
                        elif isinstance(inner_map, dict):
                            # Chain outer_map dictionary to inner map
                            # dictionary
                            maps[mtype] = deepcopy(outer_map)
                            maps[mtype].update(
                                {k: outer_map.get(v, v)
                                 for k, v in inner_map.items()})
                        else:
                            raise ArcanaDesignError(
                                "Unrecognised type for name map in '{}' "
                                "pipeline can be str or dict[str,str]: {}"
                                .format(name, inner_map))
                    else:
                        raise ArcanaDesignError(
                            "Unrecognised type for name map in '{}' "
                            "pipeline can be str or dict[str,str]: {}"
                            .format(name, outer_map))
        try:
            outer_maps = name_maps['name_maps']
        except KeyError:
            pass
        else:
            name, study, maps = self._unwrap_mods(
                outer_maps, name=name, study=study, **maps)
        return name, study, maps

    @property
    def _error_msg_loc(self):
        return "'{}' pipeline in {} class".format(self.name,
                                                  type(self.study).__name__)
