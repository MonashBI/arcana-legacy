from past.builtins import basestring
from future.utils import PY2
from builtins import object
import os
import sys
from copy import deepcopy, copy
import tempfile
import shutil
import errno
import json
from itertools import chain
from collections import defaultdict
import networkx.readwrite.json_graph as nx_json
from networkx import __version__ as networkx_version
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from logging import getLogger
from arcana.utils import extract_package_version
from arcana.__about__ import __version__
from arcana.exceptions import (
    ArcanaDesignError, ArcanaError, ArcanaUsageError, ArcanaNoConverterError,
    ArcanaDataNotDerivedYetError, ArcanaNameError)
from .provenance import (
    Record, ARCANA_DEPENDENCIES, PROVENANCE_VERSION)


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

        def my_pipeline_constructor(**name_maps):
            pipeline = self.new_pipeline('my_pipeline', name_maps=name_maps)
            pipeline.add('a_node', MyInterface())

            ...

            return pipeline

        The keywords in 'name_maps' used in pipeline construction are:

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

    def __init__(self, study, name, name_maps, desc=None, citations=None):
        name, study, maps = self._unwrap_maps(name_maps, name, study=study)
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
        self._citations = citations if citations is not None else []
        # For recording which parameters are accessed
        # during pipeline generation so they can be attributed to the
        # pipeline after it is generated (and then saved in the
        # provenance
        self._required_outputs = set()
        # Create placeholders for expected provenance records that are used
        # to compare with records saved in the repository when checking for
        # mismatches
        self._prov = None
        self._inputnodes = None
        self._outputnodes = None

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
            self._citations == other._citations)

    def __hash__(self):
        return (hash(self._name) ^
                hash(self._desc) ^
                hash(tuple(self._input_conns.keys())) ^
                hash(tuple(self._output_conns.keys())) ^
                hash(tuple(self._citations)))

    def __ne__(self, other):
        return not (self == other)

    @property
    def has_prerequisites(self):
        return any(self._study.spec(i).is_spec for i in self.inputs)

    @property
    def prerequisites(self):
        """
        Iterates through the inputs of the pipeline and determines the
        prerequisite pipelines
        """
        # Loop through the inputs to the pipeline and add the instancemethods
        # for the pipelines to generate each of the processed inputs
        prereqs = defaultdict(set)
        for input in self.inputs:  # @ReservedAssignment
            # Could be an input to the study or optional acquired spec
            if input.is_spec and input.derived:
                prereqs[input.pipeline_getter].add(input.name)
        return prereqs

    @property
    def study_inputs(self):
        """
        Returns all inputs of the study used by the pipeline, including inputs
        of prerequisites (and their prerequisites recursively)
        """
        return set(chain(
            (i for i in self.inputs if not i.derived),
            *(self.study.pipeline(p, required_outputs=r).study_inputs
              for p, r in self.prerequisites.items())))

    def map_input(self, spec_name):
        return self._map_name(spec_name, self._input_map)

    def map_output(self, spec_name):
        return self._map_name(spec_name, self._output_map)

    def add(self, name, interface, inputs=None, outputs=None,
            requirements=None, wall_time=None, annotations=None, **kwargs):
        """
        Adds a processing Node to the pipeline

        Parameters
        ----------
        name : str
            Name for the node
        interface : nipype.Interface
            The interface to use for the node
        inputs : dict[str, (str, FileFormat) | (Node, str)]
            Connections from inputs of the pipeline and outputs of other nodes
            to inputs of node. The keys of the dictionary are the field names
            and the values are 2-tuple containing either the name of the data
            spec and the data format it is expected in for pipeline inputs or
            the sending Node and the the name of an output of the sending Node.
            Note that pipeline inputs can be specified outside this method
            using the 'connect_input' method and connections between nodes with
            the the 'connect' method.
        outputs : dict[str, (str, FileFormat)]
            Connections to outputs of the pipeline from fields of the
            interface. The keys of the dictionary are the names of the data
            specs that will be written to and the values are the interface
            field name and the data format it is produced in. Note that output
            connections can also be specified using the 'connect_output'
            method.
        requirements : list(Requirement)
            List of required packages need for the node to run (default: [])
        wall_time : float
            Time required to execute the node in minutes (default: 1)
        mem_gb : int
            Required memory for the node in GB
        n_procs : int
            Preferred number of threads to run the node on (default: 1)
        annotations : dict[str, *]
            Additional annotations to add to the node, which may be used by
            the Processor node to optimise execution (e.g. 'gpu': True)
        iterfield : str
            Name of field to be passed an iterable to iterator over.
            If present, a MapNode will be created instead of a regular node
        joinsource : str
            Name of iterator field to join. Typically one of the implicit
            iterators (i.e. Study.SUBJECT_ID or Study.VISIT_ID)
            to join over the subjects and/or visits
        joinfield : str
            Name of field to pass the joined list when creating a JoinNode

        Returns
        -------
        node : Node
            The Node object that has been added to the pipeline
        """
        if annotations is None:
            annotations = {}
        if requirements is None:
            requirements = []
        if wall_time is None:
            wall_time = self.study.processor.default_wall_time
        if 'mem_gb' not in kwargs or kwargs['mem_gb'] is None:
            kwargs['mem_gb'] = self.study.processor.default_mem_gb
        if 'iterfield' in kwargs:
            if 'joinfield' in kwargs or 'joinsource' in kwargs:
                raise ArcanaDesignError(
                    "Cannot provide both joinsource and iterfield to when "
                    "attempting to add '{}' node to {}"
                    .foramt(name, self._error_msg_loc))
            node_cls = self.study.environment.node_types['map']
        elif 'joinsource' in kwargs or 'joinfield' in kwargs:
            if not ('joinfield' in kwargs and 'joinsource' in kwargs):
                raise ArcanaDesignError(
                    "Both joinsource and joinfield kwargs are required to "
                    "create a JoinNode (see {})".format(name,
                                                        self._error_msg_loc))
            joinsource = kwargs['joinsource']
            if joinsource in self.study.ITERFIELDS:
                self._iterator_joins.add(joinsource)
            node_cls = self.study.environment.node_types['join']
            # Prepend name of pipeline of joinsource to match name of nodes
            kwargs['joinsource'] = '{}_{}'.format(self.name, joinsource)
        else:
            node_cls = self.study.environment.node_types['base']
        # Create node
        node = node_cls(self.study.environment,
                        interface,
                        name="{}_{}".format(self._name, name),
                        requirements=requirements,
                        wall_time=wall_time,
                        annotations=annotations,
                        **kwargs)
        # Ensure node is added to workflow
        self._workflow.add_nodes([node])
        # Connect inputs, outputs and internal connections
        if inputs is not None:
            if not isinstance(inputs, dict):
                raise ArcanaDesignError(
                    "inputs of {} node in {} needs to be a dictionary "
                    "(not {})".format(name, self, inputs))
            for node_input, connect_from in inputs.items():
                if isinstance(connect_from[0], basestring):
                    input_spec, input_format = connect_from
                    self.connect_input(input_spec, node,
                                       node_input, input_format)
                else:
                    conn_node, conn_field = connect_from
                    self.connect(conn_node, conn_field, node, node_input)
        if outputs is not None:
            if not isinstance(outputs, dict):
                raise ArcanaDesignError(
                    "outputs of {} node in {} needs to be a dictionary "
                    "(not {})".format(name, self, outputs))
            for output_spec, (node_output, output_format) in outputs.items():
                self.connect_output(output_spec, node, node_output,
                                    output_format)
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
            prev_node, prev_node_output, _, _ = self._output_conns[name]
            logger.info(
                "Reassigning '{}' output from {}:{} to {}:{} in {}"
                .format(name, prev_node.name, prev_node_output,
                        node.name, node_output, self._error_msg_loc))
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
        return (self.study.bound_spec(i) for i in self._input_conns)

    @property
    def outputs(self):
        return (self.study.bound_spec(o) for o in self._output_conns)

    @property
    def input_names(self):
        return self._input_conns.keys()

    @property
    def output_names(self):
        return self._output_conns.keys()

    @property
    def joins(self):
        return self._iterator_joins

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
    def all_parameters(self):
        """Return all parameters, including parameters of prerequisites"""
        return chain(self.parameters, iter(self._prereq_parameters.items()))

    @property
    def non_default_parameters(self):
        return ((k, v) for k, v in self.parameters.items()
                if v != self.default_parameters[k])

    @property
    def required_outputs(self):
        return (self.study.bound_spec(o) for o in self._required_outputs)

    @property
    def desc(self):
        return self._desc

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
        node = self.workflow.get_node('{}_{}'.format(self.name, name))
        if node is None:
            raise ArcanaNameError(
                name, "{} doesn't have node named '{}'".format(self, name))
        return node

    @property
    def nodes(self):
        return (self.workflow.get_node(n)
                for n in self.workflow.list_node_names())

    @property
    def node_names(self):
        return (n[len(self.name) + 1:]
                for n in self.workflow.list_node_names())

    def save_graph(self, fname, style='flat', format='png', **kwargs):  # @ReservedAssignment @IgnorePep8
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

    def iterators(self, frequency=None):
        """
        Returns the iterators (i.e. subject_id, visit_id) that the pipeline
        iterates over

        Parameters
        ----------
        frequency : str | None
            A selected data frequency to use to determine which iterators are
            required. If None, all input frequencies of the pipeline are
            assumed
        """
        iterators = set()
        if frequency is None:
            input_freqs = list(self.input_frequencies)
        else:
            input_freqs = [frequency]
        for freq in input_freqs:
            iterators.update(self.study.FREQUENCIES[freq])
        return iterators

    def iterates_over(self, iterator, freq):
        """
        Checks to see if the given frequency requires iteration over the
        given iterator

        Parameters
        ----------
        iterator : str
            The iterator to check
        freq : str
            The frequency to check
        """
        return iterator in self.study.FREQUENCIES[freq]

    def _unwrap_maps(self, name_maps, name, study=None, **inner_maps):
        """
        Unwraps potentially nested name-mapping dictionaries to get values
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
            name, study, maps = self._unwrap_maps(
                outer_maps, name=name, study=study, **maps)
        return name, study, maps

    @property
    def _error_msg_loc(self):
        return "'{}' pipeline in {} class".format(self.name,
                                                  type(self.study).__name__)

    def inputnode(self, frequency):
        """
        Returns the input node for the given frequency.

        Parameters
        ----------
        frequency : str
            The frequency (i.e. 'per_session', 'per_visit', 'per_subject' or
            'per_study') of the input node to retrieve

        Returns
        -------
        inputnode : arcana.environment.base.Node
            The input node corresponding to the given frequency
        """
        if self._inputnodes is None:
            raise ArcanaUsageError(
                "The pipeline must be capped (see cap() method) before an "
                "input node is accessed")
        return self._inputnodes[frequency]

    def outputnode(self, frequency):
        """
        Returns the output node for the given frequency.

        Parameters
        ----------
        frequency : str
            The frequency (i.e. 'per_session', 'per_visit', 'per_subject' or
            'per_study') of the output node to retrieve

        Returns
        -------
        outputnode : arcana.environment.base.Node
            The output node corresponding to the given frequency
        """
        if self._outputnodes is None:
            raise ArcanaUsageError(
                "The pipeline must be capped (see cap() method) before an "
                "output node is accessed")
        return self._outputnodes[frequency]

    @property
    def prov(self):
        if self._prov is None:
            raise ArcanaUsageError(
                "The pipeline must be capped (see cap() method) before the "
                "provenance is accessed")
        return self._prov

    def cap(self):
        """
        "Caps" the construction of the pipeline, signifying that no more inputs
        and outputs are expected to be added and therefore the input and output
        nodes can be created along with the provenance.
        """
        to_cap = (self._inputnodes, self._outputnodes, self._prov)
        if to_cap == (None, None, None):
            self._inputnodes = {
                f: self._make_inputnode(f) for f in self.input_frequencies}
            self._outputnodes = {
                f: self._make_outputnode(f) for f in self.output_frequencies}
            self._prov = self._gen_prov()
        elif None in to_cap:
            raise ArcanaError(
                "If one of _inputnodes, _outputnodes or _prov is not None then"
                " they all should be in {}".format(self))

    def _make_inputnode(self, frequency):
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
            # Keep track of previous conversion nodes to avoid replicating the
            # conversion for inputs that are used in multiple places
            prev_conv_nodes = {}
            for (node, node_in, format,  # @ReservedAssignment @IgnorePep8
                 conv_kwargs) in self._input_conns[input.name]:
                # If fileset formats differ between study and pipeline
                # inputs create converter node (if one hasn't been already)
                # and connect input to that before connecting to inputnode
                if self.requires_conversion(input, format):
                    try:
                        conv = format.converter_from(input.format,
                                                     **conv_kwargs)
                    except ArcanaNoConverterError as e:
                        e.msg += (
                            "which is required to convert '{}' from {} to {} "
                            "for '{}' input of '{}' node in '{}' pipeline"
                            .format(
                                input.name, input.format, format, node_in,
                                node.name, self.name))
                        raise e
                    try:
                        in_node = prev_conv_nodes[format.name]
                    except KeyError:
                        in_node = prev_conv_nodes[format.name] = self.add(
                            'conv_{}_to_{}_format'.format(input.name,
                                                          format.name),
                            conv.interface,
                            inputs={conv.input: (inputnode, input.name)},
                            requirements=conv.requirements,
                            mem_gb=conv.mem_gb,
                            wall_time=conv.wall_time)
                    in_node_out = conv.output
                else:
                    in_node = inputnode
                    in_node_out = input.name
                self.connect(in_node, in_node_out, node, node_in)
        # Connect iterator inputs
        for iterator, conns in self._iterator_conns.items():
            # Check to see if this is the right frequency for the iterator
            # input, i.e. if it is the only iterator for this frequency
            if self.study.FREQUENCIES[frequency] == (iterator,):
                for (node, node_in, format) in conns:  # @ReservedAssignment
                    self.connect(inputnode, iterator, node, node_in)
        return inputnode

    def _make_outputnode(self, frequency):
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
            (node, node_out, format,  # @ReservedAssignment @IgnorePep8
             conv_kwargs) = self._output_conns[output.name]
            # If fileset formats differ between study and pipeline
            # outputs create converter node (if one hasn't been already)
            # and connect output to that before connecting to outputnode
            if self.requires_conversion(output, format):
                try:
                    conv = output.format.converter_from(format, **conv_kwargs)
                except ArcanaNoConverterError as e:
                    e.msg += (", which is required to convert '{}' output of "
                              "'{}' node in '{}' pipeline".format(
                                  output.name, node.name, self.name))
                    raise e
                node = self.add(
                    'conv_{}_from_{}_format'.format(output.name, format.name),
                    conv.interface,
                    inputs={conv.input: (node, node_out)},
                    requirements=conv.requirements,
                    mem_gb=conv.mem_gb,
                    wall_time=conv.wall_time)
                node_out = conv.output
            self.connect(node, node_out, outputnode, output.name)
        return outputnode

    def _gen_prov(self):
        """
        Extracts provenance information from the pipeline into a PipelineProv
        object

        Returns
        -------
        prov : dict[str, *]
            A dictionary containing the provenance information to record
            for the pipeline
        """
        # Export worfklow graph to node-link data format
        wf_dict = nx_json.node_link_data(self.workflow._graph)
        # Replace references to Node objects with the node's provenance
        # information and convert to a dict organised by node name to allow it
        # to be compared more easily. Also change link node-references from
        # node index to node ID so it is not dependent on the order the nodes
        # are written to the dictionary (which for Python < 3.7 is guaranteed
        # to be the same between identical runs)
        for link in wf_dict['links']:
            if int(networkx_version.split('.')[0]) < 2:  # @UndefinedVariable
                link['source'] = wf_dict['nodes'][link['source']]['id'].name
                link['target'] = wf_dict['nodes'][link['target']]['id'].name
            else:
                link['source'] = link['source'].name
                link['target'] = link['target'].name
        wf_dict['nodes'] = {n['id'].name: n['id'].prov
                            for n in wf_dict['nodes']}
        # Roundtrip to JSON to convert any tuples into lists so dictionaries
        # can be compared directly
        wf_dict = json.loads(json.dumps(wf_dict))
        dependency_versions = {d: extract_package_version(d)
                               for d in ARCANA_DEPENDENCIES}
        pkg_versions = {'arcana': __version__}
        pkg_versions.update((k, v) for k, v in dependency_versions.items()
                            if v is not None)
        prov = {
            '__prov_version__': PROVENANCE_VERSION,
            'name': self.name,
            'workflow': wf_dict,
            'study': self.study.prov,
            'pkg_versions': pkg_versions,
            'python_version': sys.version,
            'joined_ids': self._joined_ids()}
        return prov

    def expected_record(self, node):
        """
        Constructs the provenance record that would be saved in the given node
        if the pipeline was run on the current state of the repository

        Parameters
        ----------
        node : arcana.repository.tree.TreeNode
            A node of the Tree representation of the study data stored in the
            repository (i.e. a Session, Visit, Subject or Tree node)

        Returns
        -------
        expected_record : arcana.provenance.Record
            The record that would be produced if the pipeline is run over the
            study tree.
        """
        exp_inputs = {}
        # Get checksums/values of all inputs that would have been used in
        # previous runs of an equivalent pipeline to compare with that saved
        # in provenance to see if any have been updated.
        for inpt in self.inputs:  # @ReservedAssignment
            # Get iterators present in the input that aren't in this node
            # and need to be joined
            iterators_to_join = (self.iterators(inpt.frequency) -
                                 self.iterators(node.frequency))
            if not iterators_to_join:
                # No iterators to join so we can just extract the checksums
                # of the corresponding input
                exp_inputs[inpt.name] = inpt.collection.item(
                    node.subject_id, node.visit_id).checksums
            elif len(iterators_to_join) == 1:
                # Get list of checksums dicts for each node of the input
                # frequency that relates to the current node
                exp_inputs[inpt.name] = [
                    inpt.collection.item(n.subject_id, n.visit_id).checksums
                    for n in node.nodes(inpt.frequency)]
            else:
                # In the case where the node is the whole treee and the input
                # is per_seession, we need to create a list of lists to match
                # how the checksums are joined in the processor
                exp_inputs[inpt.name] = []
                for subj in node.subjects:
                    exp_inputs[inpt.name].append([
                        inpt.collection.item(s.subject_id,
                                             s.visit_id).checksums
                        for s in subj.sessions])
        # Get checksums/value for all outputs of the pipeline. We are assuming
        # that they exist here (otherwise they will be None)
        exp_outputs = {}
        for output in self.outputs:
            try:
                exp_outputs[output.name] = output.collection.item(
                    node.subject_id, node.visit_id).checksums
            except ArcanaDataNotDerivedYetError:
                pass
        exp_prov = copy(self.prov)
        if PY2:
            # Need to convert to unicode strings for Python 2
            exp_inputs = json.loads(json.dumps(exp_inputs))
            exp_outputs = json.loads(json.dumps(exp_outputs))
        exp_prov['inputs'] = exp_inputs
        exp_prov['outputs'] = exp_outputs
        exp_prov['joined_ids'] = self._joined_ids()
        return Record(
            self.name, node.frequency, node.subject_id, node.visit_id,
            self.study.name, exp_prov)

    def _joined_ids(self):
        """
        Adds the subjects/visits used to generate the derivatives iff there are
        any joins over them in the pipeline
        """
        joined_prov = {}
        if self.joins_subjects:
            joined_prov['subject_ids'] = list(self.study.subject_ids)
        if self.joins_visits:
            joined_prov['visit_ids'] = list(self.study.visit_ids)
        return joined_prov
