from .base import Processor
from nipype.pipeline.plugins import LinearPlugin


class SingleProc(Processor):
    """
    A thin wrapper around the NiPype LinearPlugin used to
    run pipelines on the local workstation on a single process

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

    nipype_plugin_cls = LinearPlugin

    num_processes = 1
