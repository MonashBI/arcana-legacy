from __future__ import division
from builtins import str  # @UnusedImports
from builtins import object
import time
import logging
from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode,
    MapNode as NipypeMapNode)

logger = logging.getLogger('arcana')

DEFAULT_MEMORY = 4096
DEFAULT_WALL_TIME = 20


class ArcanaNodeMixin(object):
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

    arcana_params = {
        'requirements': [],
        'nthreads': 1,
        'wall_time': DEFAULT_WALL_TIME,
        'memory': DEFAULT_MEMORY,
        'gpu': False,
        'environment': None}

    def __init__(self, *args, **kwargs):
        self._arcana_init(**kwargs)
        self.nipype_cls.__init__(self, *args, **kwargs)

    def _arcana_init(self, **kwargs):
        for name, value in list(self.arcana_params.items()):
            setattr(self, name, kwargs.pop(name, value))

    def _load_results(self, *args, **kwargs):
        self._load_reqs()
        result = self.nipype_cls._load_results(self, *args, **kwargs)
        self._unload_reqs()
        return result

    def _run_command(self, *args, **kwargs):
        start_time = time.time()
        try:
            self._load_reqs()
            result = self.nipype_cls._run_command(self, *args, **kwargs)
        finally:
            self._unload_reqs(not_loaded_ok=True)
        end_time = time.time()
        run_time = (end_time - start_time) // 60
        if run_time > self.wall_time:
            logger.warning("Executed '{}' node in {} minutes, which is longer "
                           "than specified wall time ({} minutes)"
                           .format(self.name, run_time, self.wall_time))
        else:
            logger.info("Executed '{}' node in {} minutes"
                        .format(self.name, run_time))
        return result

    def _load_reqs(self, **kwargs):
        if self.environment is not None:
            self.environment.load(*self.requirements, **kwargs)

    def _unload_reqs(self, **kwargs):
        if self.environment is not None:
            self.environment.unload(*self.requirements, **kwargs)


class Node(ArcanaNodeMixin, NipypeNode):

    nipype_cls = NipypeNode


class JoinNode(ArcanaNodeMixin, NipypeJoinNode):

    nipype_cls = NipypeJoinNode


class MapNode(ArcanaNodeMixin, NipypeMapNode):

    nipype_cls = NipypeMapNode

    def _make_nodes(self, cwd=None):
        """
        Cast generated nodes to be Arcana nodes
        """
        for i, node in NipypeMapNode._make_nodes(self, cwd=cwd):
            # "Cast" NiPype node to a Arcana Node and set Arcana Node
            # parameters
            node.__class__ = Node
            node._arcana_init(
                **{n: getattr(self, n) for n in self.arcana_params})
            yield i, node
