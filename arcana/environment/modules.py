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
    ArcanaRequirementNotFoundError, ArcanaVersionNotDetectableError,
    ArcanaVersionError, ArcanaModulesError)
from .base import Environment, NodeMixin, Node, JoinNode, MapNode


logger = logging.getLogger('arcana')


class ModulesNodeMixin(NodeMixin):

    def _run_command(self, *args, **kwargs):
        self.environment.load(*self.versions)
        try:
            result = self.base_cls._run_command(self, *args, **kwargs)
        finally:
            self.environment.unload(*self.versions)
        return result

    def _load_results(self, *args, **kwargs):
        self.environment.load(*self.versions)
        try:
            result = self.base_cls._load_results(self, *args, **kwargs)
        finally:
            self.environment.unload(*self.versions)
        return result


class ModulesNode(ModulesNodeMixin, Node):

    base_cls = Node  # Not req. in Py3 where super() in mixin works


class ModulesJoinNode(ModulesNodeMixin, JoinNode):

    base_cls = JoinNode  # Not req. in Py3 where super() in mixin works


class ModulesMapNode(ModulesNodeMixin, MapNode):

    node_cls = ModulesNode
    base_cls = MapNode  # Not req. in Py3 where super() in mixin works


class ModulesEnv(Environment):
    """
    An environment in which software requirements (e.g. FSL, matlab,
    MRtrix) are loaded using the 'modules' package

        Furlani, J. (1991). Modules: Providing a flexible user
        environment. Proceedings of the Fifth Large Installation Systems
        Administration Conference (LISA V), (1), 141–152.

    Parameters
    ----------
    packages_map : dct[Requirement, str] | callable
        A Mapping from the name of a requirement to a module installed in the
        environment
    versions_map : dct[Requirement, dct[str, str]] | callable
        A dictionary containing a mapping from a Requirement to dictionary,
        which in turn maps the local name of a version of a module installed in
        the environment to the standard versioning system recognised by the
        requirement.
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
        try:
            for req_range in requirements:
                req = req_range.requirement
                local_name = self.map_req(req)
                try:
                    version_names = self._available[local_name]
                except KeyError:
                    if self._fail_on_missing:
                        raise ArcanaRequirementNotFoundError(
                            "Could not find module for {} ({})".format(
                                req.name, local_name))
                    else:
                        logger.warning("Did not find module for {} ({})"
                                       .format(req.name, local_name))
                avail_versions = []
                for local_ver_name in version_names:
                    ver_name = self.map_version(req, local_ver_name)
                    try:
                        avail_versions.append(
                            req.v(ver_name, local_name=local_name,
                                  local_version=local_ver_name))
                    except ArcanaVersionNotDetectableError:
                        if self._ignore_unrecog:
                            logger.warning(
                                "Ignoring unrecognised available version '{}' "
                                "of {}".format(ver_name, req_range.name))
                            continue
                        else:
                            raise
                version = req_range.latest_within(avail_versions)
                # To get the exact version (i.e. not just what the
                # modules administrator has called it) we load the module
                # detect the version and unload it again
                if self._detect_exact_versions:
                    # Note that the versions are unloaded after the outer loop
                    # so that subsequent requirements can use previously loaded
                    # requirements to detect their version (matlab packages in
                    # particular)
                    self.load(version)
                    exact_version = req.detect_version(
                        local_name=local_name, local_version=local_ver_name)
                    try:
                        if not req_range.within(exact_version):
                            raise ArcanaVersionError(
                                "Version of {} specified by module {} does "
                                "not match expected {} and is outside the "
                                "acceptable range [{}]"
                                .format(req.name, local_ver_name,
                                        str(version), str(req_range)))
                        if exact_version < version:
                            raise ArcanaVersionError(
                                "Version of {} specified by module {} is less "
                                "than the expected {}".format(
                                    req.name, local_ver_name, str(version)))
                    finally:
                        # Append the loaded version to the list of versions to
                        # ensure that it is unloaded again before the exception
                        # is raised out of this method
                        versions.append(exact_version)
                    version = exact_version
                # Get latest requirement from list of possible options
                versions.append(version)
        finally:
            # Unload detected versions
            if self._detect_exact_versions:
                self.unload(*versions)
        return versions

    def load(self, *versions):
        for version in versions:
            self._run_module_cmd('load', self._module_id(version))

    def unload(self, *versions):
        # The modules are unloaded in reverse order to account for prerequisite
        # modules
        for version in reversed(versions):
            self._run_module_cmd('unload', self._module_id(version))

    @classmethod
    def _module_id(cls, version):
        return '{}/{}'.format(version.local_name, version.local_version)

    def map_req(self, requirement):
        """
        Maps the name of an Requirement class to the name of the corresponding
        module in the environment

        Parameters
        ----------
        requirement : Requirement
            The requirement to map to the name of a module on the system
        """
        if isinstance(self._packages_map, dict):
            local_name = self._packages_map.get(requirement, requirement.name)
        else:
            local_name = self._packages_map(requirement)
        return local_name

    def map_version(self, requirement, local_version):
        """
        Maps a local version name to one recognised by the Requirement class

        Parameters
        ----------
        requirement : str
            Name of the requirement
        version : str
            version string
        """
        if isinstance(self._versions_map, dict):
            version = self._versions_map.get(requirement, {}).get(
                local_version, local_version)
        else:
            version = self._versions_map(requirement, local_version)
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
                    raise ArcanaModulesError(
                        "Cannot find 'modulecmd' on path or in MODULESHOME.")
            logger.debug("Running modules command '{}'".format(' '.join(args)))
            try:
                output, error = sp.Popen(
                    [modulecmd, 'python'] + list(args),
                    stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            except (sp.CalledProcessError, OSError) as e:
                raise ArcanaModulesError(
                    "Call to subprocess `{}` threw an error: {}".format(
                        ' '.join([modulecmd, 'python'] + list(args)), e))
            if PY3:
                output = output.decode('utf-8')
                error = error.decode('utf-8')
            if output == '_mlstatus = False\n':
                raise ArcanaModulesError(
                    "Error running module cmd '{}':\n{}".format(
                        ' '.join(args), error))
            # Run python code generated by module load
            exec(output)
            return error
        else:
            raise ArcanaModulesNotInstalledException('MODULESHOME')
