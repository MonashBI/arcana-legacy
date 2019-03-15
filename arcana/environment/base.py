from __future__ import division
from builtins import str  # @UnusedImports
from builtins import object
import time
import logging
from nipype.interfaces.base import isdefined
from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode,
    MapNode as NipypeMapNode)
from arcana.utils import get_class_info, HOSTNAME


logger = logging.getLogger('arcana')


class NodeMixin(object):
    """
    A Mixin class used to insert additional meta data into the node (than
    the Nipype node it is used to subclass), and report accuracy of wall_time
    estimates

    Parameters
    ----------
    environment : Environment | None
        The environment within which to execute the node (automatically added
        by Arcana)
    requirements : list(Version | VersionRange)
        List of minimum software versions or range of software versions
        required
    wall_time : int
        Expected wall time of the node in minutes.
    annotations : dict[str, *]
        Flexible annotations that can be used to optimise how the node is
        executed by the processor (e.g. whether GPU cards are required)
    """

    def __init__(self, environment, *args, **kwargs):
        self._environment = environment
        # Get versions of software in the environment that satisfy the given
        # requirements
        requirements = kwargs.pop('requirements', [])
        self._versions = self._environment.satisfy(*requirements)
        self._wall_time = kwargs.pop('wall_time', None)
        self._annotations = kwargs.pop('annotations', {})
        self.nipype_cls.__init__(self, *args, **kwargs)

    def _run_command(self, *args, **kwargs):
        # Detect run time and compare against specified wall_time
        start_time = time.time()
        result = self.nipype_cls._run_command(self, *args, **kwargs)
        end_time = time.time()
        run_time = (end_time - start_time) // 60
        if run_time > self._wall_time:
            logger.warning("Executed '{}' node in {} minutes, which is longer "
                           "than specified wall time ({} minutes)"
                           .format(self.name, run_time,
                                   self._wall_time))
        else:
            logger.info("Executed '{}' node in {} minutes"
                        .format(self.name, run_time))
        return result

    @property
    def annotations(self):
        return self._annotations

    @property
    def versions(self):
        return self._versions

    @property
    def environment(self):
        return self._environment

    @property
    def prov(self):
        prov = {
            'interface': get_class_info(type(self.interface)),
            'requirements': {v.name: v.prov for v in self.versions},
            'parameters': {}}
        for trait_name in self.inputs.visible_traits():
            val = getattr(self.inputs, trait_name)
            try:
                val_prov = val.prov
            except AttributeError:
                val_prov = val
            if isdefined(val):
                prov['parameters'][trait_name] = val_prov
        return prov


class Node(NodeMixin, NipypeNode):

    nipype_cls = NipypeNode  # Not req. in Py3 where super() in mixin works


class JoinNode(NodeMixin, NipypeJoinNode):

    nipype_cls = NipypeJoinNode  # Not req. in Py3 where super() in mixin works


class MapNode(NodeMixin, NipypeMapNode):

    node_cls = Node
    nipype_cls = NipypeMapNode  # Not req. in Py3 where super() in mixin works

    def _make_nodes(self, cwd=None):
        """
        Cast generated nodes to be Arcana nodes
        """
        for i, node in NipypeMapNode._make_nodes(self, cwd=cwd):
            # "Cast" NiPype node to a Arcana Node and set Arcana Node
            # parameters
            node.__class__ = self.node_cls
            node._environment = self._environment
            node._versions = self._versions
            node._wall_time = self._wall_time
            node._annotations = self._annotations
            yield i, node


class Environment(object):
    """
    Base class for all Environment classes
    """

    node_types = {'base': Node, 'map': MapNode, 'join': JoinNode}

    def satisfy(self, *requirements):
        """
        Checks whether the given requirements are satisfiable within the given
        execution context and returns the corresponding versions that will be
        used.

        Parameter
        ---------
        requirements : list(Requirement)
            List of requirements to check whether they are satisfiable

        Returns
        -------
        versions : list(Version)
            Exact software versions to be executed in the environment
        """
        raise NotImplementedError

    @property
    def prov(self):
        return {
            'type': get_class_info(type(self)),
            'host': HOSTNAME}
