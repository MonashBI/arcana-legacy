import os
import re
import time
import math
import subprocess as sp
from collections import defaultdict
import logging
from nianalysis.exceptions import (
    NiAnalysisError, NiAnalysisModulesNotInstalledException)
from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode, MapNode as NipypeMapNode)

logger = logging.getLogger('NiAnalysis')


class NiAnalysisNodeMixin(object):
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

    def __init__(self, *args, **kwargs):
        self._requirements = kwargs.pop('requirements', [])
        self._nthreads = kwargs.pop('nthreads', 1)
        self._wall_time = kwargs.pop('wall_time', 5)
        self._memory = kwargs.pop('memory', 8000)
        self._gpu = kwargs.pop('gpu', False)
        self._loaded_modules = []
        self.nipype_cls.__init__(self, *args, **kwargs)

    def _load_results(self, *args, **kwargs):
        self._load_modules()
        result = self.nipype_cls._load_results(self, *args, **kwargs)
        self._unload_modules()
        return result

    def _run_command(self, *args, **kwargs):
        start_time = time.time()
        self._load_modules()
        result = self.nipype_cls._run_command(self, *args, **kwargs)
        self._unload_modules()
        end_time = time.time()
        run_time = (end_time - start_time) / 60
        if run_time > self._wall_time:
            logger.warning("Executed '{}' node in {} minutes, which is longer "
                           "than specified wall time ({} minutes)"
                           .format(self.name, run_time, self._wall_time))
        else:
            logger.info("Executed '{}' node in {} minutes"
                        .format(self.name, run_time))
        return result

    def _load_modules(self):
        try:
            preloaded = self._preloaded_modules()
            logger.debug("Loading required modules {} for '{}'"
                         .format(self._requirements, self.name))
            for req in self._requirements:
                try:
                    version = req.split_version(preloaded[req.name])
                    logger.debug("Found preloaded version {} of module '{}'"
                                 .format(version, req.name))
                    if not req.valid_version(version):
                        raise NiAnalysisError(
                            "Incompatible module version already loaded {}/{},"
                            " (valid {}->{}) please unload before running "
                            "pipeline"
                            .format(
                                req.name, version, req.min_version,
                                (req.max_version if req.max_version is not None
                                 else '')))
                except KeyError:
                    best_version = req.best_version(
                        self._avail_modules()[req.name])
                    logger.debug("Loading best version '{}' of module '{}' for"
                                 " requirement {}".format(best_version,
                                                          req.name, req))
                    self.load_module(req.name, best_version)
                    self._loaded_modules.append((req.name, best_version))
        except NiAnalysisModulesNotInstalledException as e:
            logger.debug("Skipping loading modules as '{}' is not set"
                         .format(e))

    def _unload_modules(self):
        try:
            for name, ver in self._loaded_modules:
                self.unload_module(name, ver)
        except NiAnalysisModulesNotInstalledException as e:
            logger.debug("Skipping unloading modules as '{}' is not set"
                         .format(e))

    @classmethod
    def _preloaded_modules(cls):
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
    def _avail_modules(cls):
        out_text = cls._run_module_cmd('avail')
        sanitized = []
        for l in out_text.split('\n'):
            if not l.startswith('-'):
                sanitized.append(l)
        avail = defaultdict(list)
        for module, ver in re.findall(r'(\w+)/([\w\d\.\-\_]+)',
                                      ' '.join(sanitized)):
            avail[module].append(ver)
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
            modulecmd = sp.check_output('which modulecmd', shell=True).strip()
            if not modulecmd:
                modulecmd = '{}/bin/modulecmd'.format(
                    os.environ['MODULESHOME'])
                if not os.path.exists(modulecmd):
                    raise NiAnalysisError(
                        "Cannot find 'modulecmd' on path or in MODULESHOME.")
            logger.debug("Running modules command '{}'".format(' '.join(args)))
            try:
                output, error = sp.Popen(
                    [modulecmd, 'python'] + list(args),
                    stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            except (sp.CalledProcessError, OSError) as e:
                raise NiAnalysisError(
                    "Call to subprocess `{}` threw an error: {}".format(
                        ' '.join([modulecmd, 'python'] + list(args)), e))
            exec output
            return error
        else:
            raise NiAnalysisModulesNotInstalledException('MODULESHOME')

    @property
    def slurm_template(self):
        return sbatch_template.format(
            wall_time=self.wall_time_str, ntasks=self._nthreads,
            memory=self._memory,
            partition=('m3c' if self._gpu else 'm3a'),
            additional=('SBATCH --gres=gpu:1\n' if self._gpu else ''))

    @property
    def wall_time_str(self):
        """
        Returns the wall time in the format required for the sbatch script
        """
        days = int(self._wall_time // 1440)
        hours = int((self._wall_time - days * 1440) // 60)
        minutes = int(math.floor(self._wall_time - days * 1440 - hours * 60))
        seconds = int((self._wall_time - math.floor(self._wall_time)) * 60)
        return "{}-{:0>2}:{:0>2}:{:0>2}".format(days, hours, minutes, seconds)


class Node(NiAnalysisNodeMixin, NipypeNode):

    nipype_cls = NipypeNode


class JoinNode(NiAnalysisNodeMixin, NipypeJoinNode):

    nipype_cls = NipypeJoinNode


class MapNode(NiAnalysisNodeMixin, NipypeMapNode):

    nipype_cls = NipypeMapNode


sbatch_template = """#!/bin/bash

# Set the partition to run the job on
#SBATCH --partition={partition}

# To set a project account for credit charging,
# SBATCH --account=pmosp

# Request CPU resource for a parallel job, for example:
#   4 Nodes each with 12 Cores/MPI processes
#SBATCH --ntasks={ntasks}
# SBATCH --ntasks-per-node=12
# SBATCH --cpus-per-task=1

# Memory usage (MB)
#SBATCH --mem-per-cpu={memory}

# Set your minimum acceptable walltime, format: day-hours:minutes:seconds
#SBATCH --time={wall_time}


# Use reserved node to run job when a node reservation is made for you already
# SBATCH --reservation=reservation_name
{additional}
"""
