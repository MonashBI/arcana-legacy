from copy import copy
from nipype.pipeline import engine as pe


class BaseRunner(object):
    """
    A thin wrapper around the NiPype LinearPlugin used to connect
    runs pipelines on the local workstation

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

    default_plugin_args = {}

    def __init__(self, work_dir, max_process_time=None,
                 reprocess=False, **kwargs):
        self._work_dir = work_dir
        self._max_process_time = max_process_time
        self._reprocess = reprocess
        self._plugin_args = copy(self.default_plugin_args)
        self._plugin_args.update(kwargs)
        self._init_plugin()

    def _init_plugin(self):
        self._plugin = self.nipype_plugin_cls(**self._plugin_args)

    def run(self, pipeline, **kwargs):
        workflow = pe.Workflow(name=pipeline.name,
                               base_dir=self.work_dir)
        pipeline.connect_to_archive(workflow, reprocess=self._reprocess,
                                    **kwargs)
        # Reset the cached tree of datasets in the archive as it will
        # change after the pipeline has run.
        pipeline.study.reset_tree()
        return workflow.run(plugin=self._plugin)

    def __repr__(self):
        return "{}(work_dir={})".format(
            type(self).__name__, self._work_dir)

    def __eq__(self, other):
        try:
            return (self._work_dir == other._work_dir and
                    (self._max_process_time ==
                     other._max_process_time) and
                    self._plugin_args == other._plugin_args)
        except AttributeError:
            return False

    @property
    def work_dir(self):
        return self._work_dir

    def __getstate__(self):
        dct = copy(self.__dict__)
        # Delete the NiPype plugin as it can be regenerated
        del dct['_plugin']
        return dct

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init_plugin()
