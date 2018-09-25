from __future__ import division
from builtins import object
import time
import math
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
    """

    arcana_params = {
        'requirements': [],
        'processor': None,
        'nthreads': 1,
        'wall_time': DEFAULT_WALL_TIME,
        'memory': DEFAULT_MEMORY,
        'gpu': False,
        'account': 'dq13'}  # Should get from processor

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
        if self.processor is not None:
            self.processor.load_requirements(*self.requirements, **kwargs)

    def _unload_reqs(self, **kwargs):
        if self.processor is not None:
            self.processor.unload_requirements(*self.requirements, **kwargs)

    @property
    def slurm_template(self):
        additional = ''
        if self.gpu:
            additional += '#SBATCH --gres=gpu:1\n'
        if self.account is not None:
            additional += '#SBATCH --account={}'.format(self.account)
        return sbatch_template.format(
            wall_time=self.wall_time_str, ntasks=self.nthreads,
            memory=self.memory,
            partition=('m3c' if self.gpu else 'm3a'),
            additional=additional)

    @property
    def wall_time_str(self):
        """
        Returns the wall time in the format required for the sbatch script
        """
        days = int(self.wall_time // 1440)
        hours = int((self.wall_time - days * 1440) // 60)
        minutes = int(math.floor(self.wall_time - days * 1440 - hours * 60))
        seconds = int((self.wall_time - math.floor(self.wall_time)) * 60)
        return "{}-{:0>2}:{:0>2}:{:0>2}".format(days, hours, minutes, seconds)


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


sbatch_template = """#!/bin/bash

# Set the partition to run the job on
#SBATCH --partition={partition}

# Request CPU resource for a parallel job, for example:
#   4 Nodes each with 12 Cores/MPI processes
#SBATCH --ntasks={ntasks}
# SBATCH --ntasks-per-node=12
# SBATCH --cpus-per-task=1

# Memory usage (MB)
#SBATCH --mem-per-cpu={memory}

# Set your minimum acceptable walltime, format: day-hours:minutes:seconds
#SBATCH --time={wall_time}

# Kill job if dependencies fail
#SBATCH --kill-on-invalid-dep=yes

# Use reserved node to run job when a node reservation is made for you already
# SBATCH --reservation=reservation_name
{additional}
"""
