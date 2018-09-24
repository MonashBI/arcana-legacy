from __future__ import division
from future.utils import PY3
import os
import re
import logging
import subprocess as sp
from collections import defaultdict
from arcana.exception import (
    ArcanaError, ArcanaModulesNotInstalledException)
from .base import RequirementManager, Requirement


logger = logging.getLogger('arcana')


class EnvModulesManager(RequirementManager):
    """
    An environment in which software requirements (e.g. FSL, matlab,
    MRtrix) are loaded using the 'modules' package

        Furlani, J. (1991). Modules: Providing a flexible user
        environment. Proceedings of the Fifth Large Installation Systems
        Administration Conference (LISA V), (1), 141–152.
    """

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
            exec(output)
            if PY3:
                error = error.decode('utf-8')
            return error
        else:
            raise ArcanaModulesNotInstalledException('MODULESHOME')
