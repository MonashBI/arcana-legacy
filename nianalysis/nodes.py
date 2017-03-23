from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode, MapNode as NipypeMapNode)
import os
import re
import subprocess as sp
from collections import defaultdict
import logging

logger = logging.getLogger('NiAnalysis')


class NiAnalysisNodeMixin(object):
    """
    A Mixin class that loads required environment modules
    (http://modules.sourceforge.net) before running the interface.
    """

    def __init__(self, *args, **kwargs):
        self._required_modules = kwargs.pop('required_modules', [])
        self._min_threads = kwargs.pop('min_threads', 1)
        self._max_nthreads = kwargs.pop('max_nthreads', 1)
        self._wall_time = kwargs.pop('wall_time', None)
        self._loaded_modules = []
        self.nipype_cls.__init__(self, *args, **kwargs)

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
    def _load_module(cls, module):
        cls._run_module_cmd('load', module)

    @classmethod
    def _unload_module(cls, module):
        cls._run_module_cmd('unload', module)

    @classmethod
    def _run_module_cmd(cls, *args):
        if 'MODULESHOME' in os.environ:
            output, error = sp.Popen(
                ['{}/bin/modulecmd'.format(os.environ['MODULESHOME']),
                 'python'] + list(args),
                stdout=sp.PIPE, stderr=sp.PIPE).communicate()
            exec output
            return error


class Node(NiAnalysisNodeMixin, NipypeNode):

    nipype_cls = NipypeNode


class JoinNode(NiAnalysisNodeMixin, NipypeJoinNode):

    nipype_cls = NipypeJoinNode


class MapNode(NiAnalysisNodeMixin, NipypeMapNode):

    nipype_cls = NipypeMapNode
