from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode, MapNode as NipypeMapNode)
import os
import subprocess as sp
import logging

logger = logging.getLogger('NiAnalysis')


class EnvModuleNodeMixin(object):
    """
    A Mixin class that loads required environment modules
    (http://modules.sourceforge.net) before running the interface.
    """

    def __init__(self, **kwargs):
        self._required_modules = kwargs.pop('requirements', [])
        self._loaded_modules = []

    def _load_results(self, *args, **kwargs):
        self._load_modules()
        self.nipype_cls._load_results(self, *args, **kwargs)
        self._unload_modules()

    def _run_command(self, *args, **kwargs):
        self._load_modules()
        self.nipype_cls._run_command(self, *args, **kwargs)
        self._unload_modules()

    def _load_modules(self):
        for req in self._required_modules:
            if req.module_name not in self._preloaded_modules():
                self._load_module(req.module_name)
                self._loaded_modules.append(req)

    def _unload_modules(self):
        for req in self._loaded_modules:
            self._unload_module(req.module_name)

    @classmethod
    def _preloaded_modules(cls):
        try:
            return os.environ['LOADEDMODULES'].split(':')
        except KeyError:
            return []

    @classmethod
    def _avail_modules(cls):
        cls._run_module_cmd('avail')

    @classmethod
    def _load_module(cls, module):
        cls._run_module_cmd('load', module)

    @classmethod
    def _unload_module(cls, module):
        cls._run_module_cmd('unload', module)

    @classmethod
    def _run_module_cmd(cls, *args):
        if 'MODULESHOME' in os.environ:
            output, _ = sp.Popen(
                ['{}/bin/modulecmd'.format(os.environ['MODULESHOME']), 'python'] + list(args),
                stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            exec output


class Node(EnvModuleNodeMixin, NipypeNode):

    nipype_cls = NipypeNode

    def __init__(self, *args, **kwargs):
        EnvModuleNodeMixin.__init__(self, **kwargs)
        self.nipype_cls.__init__(self, *args, **kwargs)


class JoinNode(EnvModuleNodeMixin, NipypeJoinNode):

    nipype_cls = NipypeJoinNode

    def __init__(self, *args, **kwargs):
        EnvModuleNodeMixin.__init__(self, **kwargs)
        self.nipype_cls.__init__(self, *args, **kwargs)


class MapNode(EnvModuleNodeMixin, NipypeMapNode):

    nipype_cls = NipypeMapNode

    def __init__(self, *args, **kwargs):
        EnvModuleNodeMixin.__init__(self, **kwargs)
        self.nipype_cls.__init__(self, *args, **kwargs)

if __name__ == '__main__':
    print EnvModuleNodeMixin._preloaded_modules()
    EnvModuleNodeMixin._load_module('mrtrix')
    print EnvModuleNodeMixin._preloaded_modules()
    print sp.check_output('mrinfo', shell=True)
