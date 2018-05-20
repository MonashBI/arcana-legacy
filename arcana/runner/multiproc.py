from .base import BaseRunner
from nipype.pipeline.plugins import MultiProcPlugin


class MultiProcRunner(BaseRunner):
    """
    Wrapper around the NiPype MultiProcPlugin

    Parameters
    ----------
    """

    nipype_plugin_cls = MultiProcPlugin

    def __init__(self, work_dir, num_processes=None, plugin_args=None,
                 **kwargs):
        if plugin_args is None:
            plugin_args = {}
        if num_processes is not None:
            plugin_args['n_procs'] = num_processes
        super(MultiProcRunner, self).__init__(
            work_dir, plugin_args=plugin_args, **kwargs)

    @property
    def num_processes(self):
        return self._plugin.processes
