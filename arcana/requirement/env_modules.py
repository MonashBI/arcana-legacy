from __future__ import division
from future.utils import PY3
import os
import re
import logging
import subprocess as sp
from collections import defaultdict
from arcana.exception import (
    ArcanaError, ArcanaModulesNotInstalledException,
    ArcanaEnvModuleNotLoadedError)
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

    def __init__(self, *args, **kwargs):
        super(EnvModulesManager, self).__init__(*args, **kwargs)
        self._loaded = {}
        self._available = None
        self._preloaded = None

    def load(self, *requirements):

        for req in requirements:
            # Get best requirement from list of possible options
            name, version = Requirement.best_requirement(
                req, self.available, self.preloaded)
            module_id = name + ('/' + version if version is not None else '')
            self._run_module_cmd('load', module_id)
            self._loaded[req] = module_id

    def unload(self, *requirements, not_loaded_ok=False):
        for req in requirements:
            try:
                module_id = self._loaded[req]
            except KeyError:
                if not not_loaded_ok:
                    raise ArcanaEnvModuleNotLoadedError(
                        "Could not unload module ({}) as it wasn't loaded"
                        .format(req))
        self._run_module_cmd('unload', module_id)

    @property
    def preloaded(self):
        if self._preloaded is None:
            self._preloaded = {}
            loaded = os.environ.get('LOADEDMODULES', [])
            for modstr in loaded.split(':'):
                name, versionstr = modstr.split('/')
                self._preloaded[name] = versionstr
        return self._preloaded

    @property
    def available(self):
        if self._available is None:
            out_text = self._run_module_cmd('avail')
            sanitized = []
            for l in out_text.split('\n'):
                if not l.startswith('-'):
                    sanitized.append(l)
            self._available = defaultdict(list)
            for module, ver in re.findall(r'(\w+)/([\w\d\.\-\_]+)',
                                          ' '.join(sanitized)):
                self._available[module.lower()].append(ver)
        return self._available

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
