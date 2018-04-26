from .base import BaseRunner
from nipype.pipeline.plugins import MultiProcPlugin


class MultiProcRunner(BaseRunner):
    """
    Wrapper around the NiPype MultiProcPlugin

    Parameters
    ----------
    """

    nipype_plugin_cls = MultiProcPlugin

    def __init__(self, work_dir, num_processes=None, **kwargs):
        if num_processes is not None:
            kwargs['n_procs'] = num_processes
        super(MultiProcRunner, self).__init__(work_dir, **kwargs)

    @property
    def num_processes(self):
        return self._plugin.processes
