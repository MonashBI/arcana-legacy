

class BaseRunner(object):
    """
    Connects pipeline to archive and runs it on the local workstation

    Parameters
    ----------
    work_dir : str
        A directory in which to run the nipype workflows
    max_process_time : float
        The maximum time allowed for the process
    reprocess: True|False|'all'
        A flag which determines whether to rerun the processing for this
        step. If set to 'all' then pre-requisite pipelines will also be
        reprocessed.
    """

    plugin_args = {}

    def __init__(self, work_dir, max_process_time):
        self._work_dir = work_dir
        self._max_process_time = max_process_time
        self._plugin = self.nipype_plugin_cls(**self.plugin_args)

    def run(self):
        pass
