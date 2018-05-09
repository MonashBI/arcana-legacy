import os
from arcana.exception import (
    ArcanaError, ArcanaJobSubmittedException)
from .base import BaseRunner
from nipype.pipeline.plugins.slurmgraph import SLURMGraphPlugin


class ArcanaSlurmGraphPlugin(SLURMGraphPlugin):

    def _get_args(self, node, keywords):
        """
        Intercept calls to get template and return our own node-specific
        template
        """
        args = super(ArcanaSlurmGraphPlugin, self)._get_args(
            node, keywords)
        # Substitute the template arg with the node-specific one
        args = tuple((node.slurm_template if k == 'template' else a)
                     for k, a in zip(keywords, args))
        return args


class SlurmRunner(BaseRunner):

    nipype_plugin_cls = ArcanaSlurmGraphPlugin

    def __init__(self, work_dir, email=None, mail_on=('FAIL',),
                 **kwargs):
        if email is None:
            try:
                email = os.environ['EMAIL']
            except KeyError:
                raise ArcanaError(
                    "'email' kwarg needs to be provided for SlurmRunner"
                    " if 'EMAIL' environment variable not set")
        self._email = email
        self._mail_on = mail_on
        pargs = [('mail-user', email)]
        for mo in mail_on:
            pargs.append(('mail-type', mo))
        super(SlurmRunner, self).__init__(
            work_dir, sbatch_args=' '.join(
                '--{}={}'.format(*a) for a in pargs), **kwargs)

    @property
    def email(self):
        return self._email

    @property
    def mail_on(self):
        return self._mail_on

    def run(self, pipeline, **kwargs):
        super(SlurmRunner, self).run(pipeline, **kwargs)
        raise ArcanaJobSubmittedException(
            "Pipeline '{}' has been submitted to SLURM scheduler "
            "for processing. Please run script again after the jobs "
            "have been successful.")
