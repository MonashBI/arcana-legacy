from .base import BaseRunner
from nipype.pipeline.plugins import MultiProcPlugin


class MultiProcRunner(BaseRunner):

    nipype_plugin_cls = MultiProcPlugin
