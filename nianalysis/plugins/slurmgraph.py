from nipype.pipeline.plugins.slurmgraph import (
    SLURMGraphPlugin as BaseSLURMGraphPlugin)


class SLURMGraphPlugin(BaseSLURMGraphPlugin):

    def _get_args(self, node, keywords):
        """
        Intercept calls to get template and return our own node-specific
        template
        """
        args = super(SLURMGraphPlugin, self)._get_args(node, keywords)
        try:
            template_index = keywords.index('template')
        except ValueError:
            return args
        args[template_index] = node.slurm_template
        return args
