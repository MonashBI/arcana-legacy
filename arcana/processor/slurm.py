from builtins import zip
import math
import os
from arcana.exceptions import (
    ArcanaError, ArcanaJobSubmittedException)
from .base import Processor
from nipype.pipeline.plugins.slurmgraph import SLURMGraphPlugin


class ArcanaSlurmGraphPlugin(SLURMGraphPlugin):

    def __init__(self, *args, **kwargs):
        self._processor = kwargs.pop('processor')
        super(ArcanaSlurmGraphPlugin, self).__init__(*args, **kwargs)

    def _get_args(self, node, keywords):
        """
        Intercept calls to get template and return our own node-specific
        template
        """
        args = super(ArcanaSlurmGraphPlugin, self)._get_args(
            node, keywords)
        # Substitute the template arg with the node-specific one
        new_args = []
        for name, arg in zip(keywords, args):
            if name == 'template':
                new_args.append(self._processor.slurm_template(node))
            else:
                new_args.append(arg)
        return tuple(new_args)


class SlurmProc(Processor):
    """
    A thin wrapper around the NiPype SLURMGraphPlugin used to connect
    submit pipelines to a Slurm scheduler

    Parameters
    ----------
    work_dir : str
        A directory in which to run the nipype workflows
    partition : str | function
        Either a string naming the partition to use for the jobs or a function
        that takes a node as an argument and returns the name of the partition
        to use for that particular node.
    account : str
        Name of the account to submit the jobs against.
    email : str | None
        The email address to send alerts to. If not provided the 'EMAIL'
        environment variable needs to be set
    generic_resources : function
        A function that takes a node as an argument and returns the list of
        generic resources (e.g. GPU, bandwidth) required
    mail_on : List[str]
        Conditions on which to send mail (default 'FAIL')
    max_process_time : float
        The maximum time allowed for the process
    reprocess: True|False|'all'
        A flag which determines whether to rerun the processing for this
        step. If set to 'all' then pre-requisite pipelines will also be
        reprocessed.
    """

    nipype_plugin_cls = ArcanaSlurmGraphPlugin

    def __init__(self, work_dir, partition=None, account=None, email=None,
                 mail_on=('FAIL',), generic_resources=None,
                 ntasks_per_node=None, cpus_per_task=None, **kwargs):
        if email is None:
            try:
                email = os.environ['EMAIL']
            except KeyError:
                raise ArcanaError(
                    "'email' kwarg needs to be provided for SlurmProc"
                    " if 'EMAIL' environment variable not set")
        self._email = email
        self._mail_on = mail_on
        self._account = account
        self._partition = partition
        self._ntasks_per_node = ntasks_per_node
        self._cpus_per_task = cpus_per_task
        self._generic_resources = generic_resources
        super(SlurmProc, self).__init__(work_dir, **kwargs)

    def _init_plugin(self):
        self._plugin = self.nipype_plugin_cls(processor=self,
                                              **self._plugin_args)

    @property
    def email(self):
        return self._email

    @property
    def mail_on(self):
        return self._mail_on

    @property
    def account(self):
        return self._account

    def run(self, pipeline, **kwargs):
        super(SlurmProc, self).run(pipeline, **kwargs)
        raise ArcanaJobSubmittedException(
            "Pipeline '{}' has been submitted to SLURM scheduler "
            "for processing. Please run script again after the jobs "
            "have been successful.")

    def slurm_template(self, node):
        sbatch = self.sbatch_template.format(
            wall_time=self.wall_time_str(node.wall_time), ntasks=node.n_procs,
            memory=int(node.mem_gb * 1000),
            email=self.email,
            account=self.account)
        if self.account is not None:
            sbatch += ("\n# Set the account\n"
                       "#SBATCH --account={}\n".format(self.account))
        if self._partition is not None:
            sbatch += ("\n# Set the partition to run the job on\n"
                       "#SBATCH --partition={}\n".format(
                           self._partition(node) if callable(self._partition)
                           else self._partition))
        if self._generic_resources is not None:
            sbatch += ("\n# Request generic resources\n")
            for gres in self._generic_resources(node):
                sbatch += '#SBATCH --gres={}\n'.format(gres)
        if self._mail_on:
            sbatch += ("\n# Set mail triggers\n")
            for mo in self._mail_on:
                sbatch += '#SBATCH --mail-type={}\n'.format(mo)
        sbatch += "\n# Node and CPU options\n"
        if self._cpus_per_task is not None:
            sbatch += "#SBATCH --cpus-per-task={}\n".format(
                self._cpus_per_task)
        if self._ntasks_per_node is not None:
            sbatch += "#SBATCH --ntasks-per-node={}\n".format(
                self._ntasks_per_node)
        return sbatch

    def wall_time_str(self, wall_time):
        """
        Returns the wall time in the format required for the sbatch script
        """
        days = int(wall_time // 1440)
        hours = int((wall_time - days * 1440) // 60)
        minutes = int(math.floor(wall_time - days * 1440 - hours * 60))
        seconds = int((wall_time - math.floor(wall_time)) * 60)
        return "{}-{:0>2}:{:0>2}:{:0>2}".format(days, hours, minutes, seconds)

    sbatch_template = """#!/bin/bash

# Set the email
#SBATCH --email={email}

# Request CPU resource for a parallel job
#SBATCH --ntasks={ntasks}

# Memory usage (MB)
#SBATCH --mem-per-cpu={memory}

# Set your minimum acceptable walltime, format: day-hours:minutes:seconds
#SBATCH --time={wall_time}

# Kill job if dependencies fail
#SBATCH --kill-on-invalid-dep=yes
"""
