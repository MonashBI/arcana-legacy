import os
import re
import time
import math
import subprocess as sp
from collections import defaultdict
import logging
from arcana.requirement import Requirement
from arcana.exception import (
    ArcanaError, ArcanaModulesNotInstalledException)
from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode,
    MapNode as NipypeMapNode)

logger = logging.getLogger('Arcana')

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
        'nthreads': 1,
        'wall_time': DEFAULT_WALL_TIME,
        'memory': DEFAULT_MEMORY,
        'gpu': False,
        'account': 'dq13'}  # Should get from runner

    def __init__(self, *args, **kwargs):
        self._arcana_init(**kwargs)
        self.nipype_cls.__init__(self, *args, **kwargs)

    def _arcana_init(self, **kwargs):
        for name, value in self.arcana_params.items():
            setattr(self, name, kwargs.pop(name, value))
        self._loaded_modules = []

    def _load_results(self, *args, **kwargs):
        self._load_modules()
        result = self.nipype_cls._load_results(self, *args, **kwargs)
        self._unload_modules()
        return result

    def _run_command(self, *args, **kwargs):
        start_time = time.time()
        try:
            self._load_modules()
            result = self.nipype_cls._run_command(self, *args, **kwargs)
        finally:
            self._unload_modules()
        end_time = time.time()
        run_time = (end_time - start_time) / 60
        if run_time > self.wall_time:
            logger.warning("Executed '{}' node in {} minutes, which is longer "
                           "than specified wall time ({} minutes)"
                           .format(self.name, run_time, self.wall_time))
        else:
            logger.info("Executed '{}' node in {} minutes"
                        .format(self.name, run_time))
        return result

    def _load_modules(self):
        try:
            preloaded = self.preloaded_modules()
            available = self.available_modules()
            logger.debug("Loading required modules {} for '{}'"
                         .format(self.requirements, self.name))
            for possible_reqs in self.requirements:
                # Get best requirement from list of possible options
                req_name, req_ver = Requirement.best_requirement(
                    possible_reqs, available, preloaded)
                # Load best requirement
                self.load_module(name=req_name, version=req_ver)
                # Register best requirement
                self._loaded_modules.append((req_name, req_ver))
        except ArcanaModulesNotInstalledException as e:
            logger.debug("Skipping loading modules as '{}' is not set"
                         .format(e))

    def _unload_modules(self):
        try:
            for name, ver in self._loaded_modules:
                self.unload_module(name, ver)
        except ArcanaModulesNotInstalledException as e:
            logger.debug("Skipping unloading modules as '{}' is not set"
                         .format(e))

    @classmethod
    def preloaded_modules(cls):
        loaded = os.environ.get('LOADEDMODULES', [])
        if not loaded:
            modules = {}
        else:
            modules = {}
            for modstr in loaded.split(':'):
                name, versionstr = modstr.split('/')
                modules[name] = versionstr
        return modules

    @classmethod
    def available_modules(cls):
        out_text = cls._run_module_cmd('avail')
        sanitized = []
        for l in out_text.split('\n'):
            if not l.startswith('-'):
                sanitized.append(l)
        avail = defaultdict(list)
        for module, ver in re.findall(r'(\w+)/([\w\d\.\-\_]+)',
                                      ' '.join(sanitized)):
            avail[module.lower()].append(ver)
        return avail

    @classmethod
    def load_module(cls, name, version=None):
        cls._run_module_cmd('load', cls._version_str(name, version))

    @classmethod
    def unload_module(cls, name, version=None):
        cls._run_module_cmd('unload', cls._version_str(name, version))

    @classmethod
    def _version_str(cls, name, version=None):
        return name + ('/' + version if version is not None else '')

    @classmethod
    def _run_module_cmd(cls, *args):
        if 'MODULESHOME' in os.environ:
            try:
                modulecmd = sp.check_output('which modulecmd',
                                            shell=True).strip()
            except sp.CalledProcessError:
                modulecmd = False
            if not modulecmd:
                modulecmd = '{}/bin/modulecmd'.format(
                    os.environ['MODULESHOME'])
                if not os.path.exists(modulecmd):
                    raise ArcanaError(
                        "Cannot find 'modulecmd' on path or in MODULESHOME.")
            logger.debug("Running modules command '{}'".format(' '.join(args)))
            try:
                output, error = sp.Popen(
                    [modulecmd, 'python'] + list(args),
                    stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            except (sp.CalledProcessError, OSError) as e:
                raise ArcanaError(
                    "Call to subprocess `{}` threw an error: {}".format(
                        ' '.join([modulecmd, 'python'] + list(args)), e))
            exec output
            return error
        else:
            raise ArcanaModulesNotInstalledException('MODULESHOME')

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
