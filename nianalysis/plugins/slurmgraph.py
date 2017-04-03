from nipype.pipeline.plugins.slurmgraph import (
    SLURMGraphPlugin as BaseSLURMGraphPlugin)


class SLURMGraphPlugin(BaseSLURMGraphPlugin):

    def _get_args(self, node, keywords):
        """
        Intercept calls to get template and return our own node-specific
        template
        """
        args = super(SLURMGraphPlugin, self)._get_args(node, keywords)
        # Substitute the template arg with the node-specific one
        args = tuple((node.slurm_template if k == 'template' else a)
                     for k, a in zip(keywords, args))
        return args
