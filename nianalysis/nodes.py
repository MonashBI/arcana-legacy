from nipype.pipeline.engine import (
    Node as NipypeNode, JoinNode as NipypeJoinNode, MapNode as NipypeMapNode)
import os
import re
import subprocess as sp
from collections import defaultdict
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
    test_output = """
---------------------------- /environment/modules/ -----------------------------
ants/2.1.0                      neuron/7.4p
dcm2niix/7-2-2017               neurosim/1.0
fix1.06/1.06                    nianalysis_scripts/1.0(default)
freesurfer/5.3                  niftimatlib/1.2
fsl/5.0.8p(default)             noddi/0.9
fsl/5.0.9                       qt/4
fsl/parnesh                     R/3.3.2
group_icat/4.0a                 rest/1.8
itk/4.10.0(default)             rwmhseg/master
matlab/R2015b(default)          spm/12(default)
mcr/8.3                         spm/8
mrtrix/3(default)               w2mhs/2.1
mulan/master                    w2mhs-itk/1.0(default)
nest/2.10.0                     xnat-utils/0.1
neuron/7.4"""
#    print re.findall(r'(\w+)/([^\s]+)', test_output)
    print EnvModuleNodeMixin._avail_modules()
