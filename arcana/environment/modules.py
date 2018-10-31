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
    ArcanaEnvModuleNotLoadedError, ArcanaRequirementNotFoundError,
    ArcanaVersionNotDectableError)
from .static import StaticEnvironment


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
    packages_map : dct[str, str] | callable
        A Mapping from the name of a requirement to a module installed in the
        environment
    versions_map : dct[str, dct[str, str]] | callable
        A Mapping from the name of a requirement to a dictionary, which in
        turn maps the version of a module installed in the environment to
        a version recognised by the requirement.
    ignore_unrecognised : bool
        If True, then unrecognisable versions are ignored instead of
        throwing an error
    """

    def __init__(self, packages_map=None, versions_map=None,
                 ignore_unrecognised=True):
        self._loaded = {}
        if packages_map is None:
            packages_map = {}
        if versions_map is None:
            versions_map = {}
        self._avail_cache = None
        self._packages_map = packages_map
        self._versions_map = versions_map
        self._available = self.available()
        self._ignore_unrecog = ignore_unrecognised

    def satisfy(self, *requirements):
        versions = []
        for req_range in requirements:
            local_name = self._packages_map.get(req_range.name,
                                                 req_range.name)
            try:
                version_names = self.available[local_name]
            except KeyError:
                raise ArcanaRequirementNotFoundError(
                    "Could not find module for {} ({})".format(req_range.name,
                                                               local_name))
            versions_map = self._versions_map.get(req_range.name, {})
            avail_versions = []
            for local_ver_name in version_names:
                ver_name = versions_map.get(local_ver_name, local_ver_name)
                try:
                    avail_versions.append(
                        req_range.requirement.v(ver_name,
                                                raw_name=local_name,
                                                raw_version=local_ver_name))
                except ArcanaVersionNotDectableError:
                    if self._ignore_unrecog:
                        logger.warning(
                            "Ignoring unrecognised available version '{}' of "
                            "{}".format(ver_name, req_range.name))
                        continue
                    else:
                        raise
            # Get latest requirement from list of possible options
            versions.append(req_range.latest_within(avail_versions))
        return versions

    def load(self, *versions):
        for version in versions:
            self._run_module_cmd('load', self._module_id(version))

    def unload(self, *versions):
        for version in versions:
            self._run_module_cmd('unload', self._module_id(version))

    @classmethod
    def _module_id(cls, version):
        return '{}/{}'.format(version.raw_name, version.raw_version)

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
