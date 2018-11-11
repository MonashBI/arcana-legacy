# -*- coding: utf-8 -*-
from __future__ import division
from future.utils import PY3
from builtins import str  # @UnusedImport
import os
import re
import logging
import subprocess as sp
from collections import defaultdict
from arcana.exceptions import (
    ArcanaError, ArcanaModulesNotInstalledException,
    ArcanaRequirementNotFoundError, ArcanaVersionNotDectableError,
    ArcanaVersionError)
from .base import BaseEnvironment, NodeMixin, Node, JoinNode, MapNode


logger = logging.getLogger('arcana')


class ModulesNodeMixin(NodeMixin):

    def _run_command(self, *args, **kwargs):
        try:
            self.environment.load(*self.versions)
            result = self.base_cls._run_command(self, *args, **kwargs)
        finally:
            self.environment.unload(*self.versions)
        return result

    def _load_results(self, *args, **kwargs):
        self.environment.load(*self.versions)
        result = self.base_cls._load_results(self, *args, **kwargs)
        self.environment.unload(*self.versions)
        return result


class ModulesNode(ModulesNodeMixin, Node):

    base_cls = Node  # Not req. in Py3 where super() in mixin works


class ModulesJoinNode(ModulesNodeMixin, JoinNode):

    base_cls = JoinNode  # Not req. in Py3 where super() in mixin works


class ModulesMapNode(ModulesNodeMixin, MapNode):

    node_cls = ModulesNode
    base_cls = MapNode  # Not req. in Py3 where super() in mixin works


class ModulesEnvironment(BaseEnvironment):
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

    node_types = {'base': ModulesNode, 'map': ModulesMapNode,
                  'join': ModulesJoinNode}

    def __init__(self, packages_map=None, versions_map=None,
                 fail_on_missing=True, ignore_unrecognised=True,
                 detect_exact_versions=True):
        if packages_map is None:
            packages_map = {}
        if versions_map is None:
            versions_map = {}
        self._packages_map = packages_map
        self._versions_map = versions_map
        self._ignore_unrecog = ignore_unrecognised
        self._fail_on_missing = fail_on_missing
        self._detect_exact_versions = detect_exact_versions
        self._detected_cache = None
        self._available = self.available()

    def __eq__(self, other):
        return (self._packages_map == other._packages_map and
                self._versions_map == other._versions_map and
                self._fail_on_missing == other._fail_on_missing and
                self._ignore_unrecog == other._ignore_unrecog and
                self._detect_exact_version == other._detect_exact_version)

    def satisfy(self, *requirements):
        versions = []
        for req_range in requirements:
            req = req_range.requirement
            local_name = self.map_name(req.name)
            try:
                version_names = self._available[local_name]
            except KeyError:
                if self._fail_on_missing:
                    raise ArcanaRequirementNotFoundError(
                        "Could not find module for {} ({})".format(req.name,
                                                                   local_name))
                else:
                    logger.warning("Did not find module for {} ({})"
                                   .format(req.name, local_name))
            avail_versions = []
            for local_ver_name in version_names:
                ver_name = self.map_version(req_range.name, local_ver_name)
                try:
                    avail_versions.append(
                        req.v(ver_name, local_name=local_name,
                              local_version=local_ver_name))
                except ArcanaVersionNotDectableError:
                    if self._ignore_unrecog:
                        logger.warning(
                            "Ignoring unrecognised available version '{}' of "
                            "{}".format(ver_name, req_range.name))
                        continue
                    else:
                        raise
            version = req_range.latest_within(avail_versions)
            # To get the exact version (i.e. not just what the
            # modules administrator has called it) we load the module
            # detect the version and unload it again
            if self._detect_exact_versions:
                self.load(version)
                exact_version = req.detect_version(
                    local_name=local_name, local_version=local_ver_name)
                self.unload(version)
                if not req_range.within(exact_version):
                    raise ArcanaVersionError(
                        "Version of {} specified by module {} does not match "
                        "expected {} and is outside the acceptable range [{}]"
                        .format(req.name, local_ver_name, str(version),
                                str(req_range)))
                if exact_version < version:
                    raise ArcanaVersionError(
                        "Version of {} specified by module {} is less than "
                        "the expected {}"
                        .format(req.name, local_ver_name, str(version)))
                version = exact_version
            # Get latest requirement from list of possible options
            versions.append(version)
        return versions

    def load(self, *versions):
        for version in versions:
            self._run_module_cmd('load', self._module_id(version))

    def unload(self, *versions):
        for version in versions:
            self._run_module_cmd('unload', self._module_id(version))

    @classmethod
    def _module_id(cls, version):
        return '{}/{}'.format(version.local_name, version.local_version)

    def map_name(self, name):
        """
        Maps the name of an Requirement class to the name of the corresponding
        module in the environment
        """
        if isinstance(self._packages_map, dict):
            local_name = self._packages_map.get(name, name)
        else:
            local_name = self._packages_map(name)
        return local_name

    def map_version(self, name, local_version):
        """
        Maps a local version name to one recognised by the Requirement class

        Parameters
        ----------
        name : str
            Name of the requirement
        local_version : str
            version string
        """
        if isinstance(self._versions_map, dict):
            version = self._versions_map.get(name, {}).get(local_version,
                                                           local_version)
        else:
            version = self._versions_map(name, local_version)
        return version

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
