from .base import BaseRunner
from nipype.pipeline.plugins import MultiProcPlugin


class MultiProcRunner(BaseRunner):
    """
    A thin wrapper around the NiPype MultiProcPlugin used to
    run pipelines on the local workstation on muliple processes

    Parameters
    ----------
    work_dir : str
        A directory in which to run the nipype workflows
    num_processes : int
        The number of processes to use
    max_process_time : float
        The maximum time allowed for the process
    reprocess: True|False|'all'
        A flag which determines whether to rerun the processing for this
        step. If set to 'all' then pre-requisite pipelines will also be
        reprocessed.
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
