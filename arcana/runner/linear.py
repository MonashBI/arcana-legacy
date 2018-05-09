from .base import BaseRunner
from nipype.pipeline.plugins import LinearPlugin


class LinearRunner(BaseRunner):

    nipype_plugin_cls = LinearPlugin

    num_processes = 1
