from .base import BaseRunner
from nipype.pipeline.plugins.slurmgraph import SLURMGraphPlugin


class SlurmGraphPlugin(SLURMGraphPlugin):

    def _get_args(self, node, keywords):
        """
        Intercept calls to get template and return our own node-specific
        template
        """
        args = super(SlurmGraphPlugin, self)._get_args(node, keywords)
        # Substitute the template arg with the node-specific one
        args = tuple((node.slurm_template if k == 'template' else a)
                     for k, a in zip(keywords, args))
        return args


class SlurmRunner(BaseRunner):

    nipype_plugin_cls = SlurmGraphPlugin

    def __init__(self, email=None, mail_on=('FAIL',)):
        pass
