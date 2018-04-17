from .base import BaseRunner
from nipype.pipeline.plugins import MultiProcPlugin


class MultiProcRunner(BaseRunner):

    nipype_plugin_cls = MultiProcPlugin

    def __init__(self, work_dir, num_processes, **kwargs):
        super(MultiProcRunner, self).__init__(work_dir, **kwargs)
        
        
