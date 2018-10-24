# -*- coding: utf-8 -*-
from __future__ import division
from future.utils import PY3
from builtins import str  # @UnusedImport
import os
import re
import logging
import subprocess as sp
from collections import defaultdict
from arcana.exception import (
    ArcanaError, ArcanaModulesNotInstalledException,
    ArcanaEnvModuleNotLoadedError)
from .static import StaticEnvironment
from .requirement import Requirement


logger = logging.getLogger('arcana')


class ModulesEnvironment(StaticEnvironment):
    """
    An environment in which software requirements (e.g. FSL, matlab,
    MRtrix) are loaded using the 'modules' package

        Furlani, J. (1991). Modules: Providing a flexible user
        environment. Proceedings of the Fifth Large Installation Systems
        Administration Conference (LISA V), (1), 141–152.

    Parameters
    ----------
    packages_map : dct[str, Requirement] | callable
        A Mapping from the name a module installed on the system to a
        requirement
    versions_map : dct[str, str] | callable
        A Mapping from the name of the version to the standarding versioning
        conventions assumed by the corresponding requirement
    """

    def __init__(self, packages_map=None, versions_map=None):
        self._loaded = {}
        if packages_map is None:
            packages_map = {}
        if versions_map is None:
            versions_map = {}
        self._avail_cache = None
        self._packages_map = packages_map
        self._versions_map = versions_map

    def load(self, *requirements):
        for req in requirements:
            # Get best requirement from list of possible options
            name, version = Requirement.best_requirement(
                req, self._available_cache)
            module_id = name + ('/' + version if version is not None else '')
            self._run_module_cmd('load', module_id)
            self._loaded[req] = module_id

    def unload(self, *requirements, **kwargs):
        not_loaded_ok = kwargs.pop('not_loaded_ok', False)
        for req in requirements:
            try:
                module_id = self._loaded[req]
            except KeyError:
                if not not_loaded_ok:
                    raise ArcanaEnvModuleNotLoadedError(
                        "Could not unload module ({}) as it wasn't loaded"
                        .format(req))
            else:
                self._run_module_cmd('unload', module_id)

    @classmethod
    def loaded(cls):
        loaded = {}
        loaded_str = os.environ.get('LOADEDMODULES', '')
        if loaded_str:
            for modstr in loaded_str.split(':'):
                parts = modstr.split('/')
                if len(parts) == 2:
                    name, versionstr = parts
                else:
                    name = parts[0]
                    versionstr = None
                loaded[name] = versionstr
        return loaded

    @classmethod
    def available(cls):
        out_text = cls._run_module_cmd('avail')
        sanitized = []
        for l in out_text.split('\n'):
            if not l.startswith('-'):
                sanitized.append(l)
        available = defaultdict(list)
        for module, ver in re.findall(r'(\w+)/([\w\d\.\-\_]+)',
                                      ' '.join(sanitized)):
            available[module.lower()].append(ver)
        return available

    @property
    def _available_cache(self):
        if self._avail_cache is None:
            self._avail_cache = self.available()
        return self._avail_cache

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
