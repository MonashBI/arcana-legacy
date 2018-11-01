from __future__ import division
from builtins import str  # @UnusedImports
from builtins import object
import time
import logging
from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode,
    MapNode as NipypeMapNode)


logger = logging.getLogger('arcana')


class NodeMixin(object):
    """
    A Mixin class that loads required environment modules
    (http://modules.sourceforge.net) before running the interface.

    Parameters
    ----------
    requirements : list(Requirements)
        List of required modules to be loaded (if environment modules is
        installed)
    wall_time : int
        Expected wall time of the node in minutes.
    memory : int
        Required memory for the node (in MB).
    nthreads : int
        Preferred number of threads (ignored if processed in single node)
    gpu : bool
        Flags whether a GPU compute node is preferred
    environment : Environment | None
        The environment within which to execute the node (automatically added
        by Arcana)
    """

    def __init__(self, environment, versions, wall_time, annotations,
                 *args, **kwargs):
        self.nipype_cls.__init__(self, *args, **kwargs)
        self._environment = environment
        self._versions = versions
        self._wall_time = wall_time
        self._annotations = annotations

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


class BaseEnvironment(object):
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

    def make_node(self, node_type, requirements, annotations, *args, **kwargs):
        if node_type == 'map':
            node_cls = self.MapNode
        elif node_type == 'join':
            node_cls = self.JoinNode
        else:
            node_cls = self.Node
        versions = self.satisfy(requirements)
        return node_cls(self, versions, annotations, *args, **kwargs)
