from future.utils import PY3
from .base import Requirement
import subprocess as sp
from arcana.exception import (
    ArcanaUsageError, ArcanaRequirementNotFoundError)


class CliRequirement(Requirement):
    """
    Defines a software package that is available on the command line

    Parameters
    ----------
    name : str
        Name of the package
    min_version : tuple(int|str)
        The minimum version required by the node
    max_version : tuple(int|str) | None
        The maximum version that is compatible with the Node
    test_cmd : str
        The name of a command that should be available when the
        requirement is satisfied.
    version_split : function
        A function that splits the version string into major/minor/micro
        parts or equivalent
    references : list[Citation]
        A list of references that should be cited when using this software
        requirement
    """

    def __init__(self, name, test_cmd, version_switch='--version', **kwargs):
        super(CliRequirement, self).__init__(name, **kwargs)
        self._test_cmd = test_cmd
        self._version_switch = version_switch

    def detect_version(self):
        test_cmd_loc = self.locate_command(self._test_cmd)
        try:
            version_str = sp.check_output(
                '{} {}'.format(test_cmd_loc, self._version_switch), shell=True)
        except sp.CalledProcessError as e:
            raise ArcanaUsageError(
                "Unrecognised version switch '{}' for {}:\n{}".format(
                    self._version_switch, self, e))
        if PY3:
            version_str = version_str.decode('utf-8')
        return self.parse_version(version_str)

    def locate_command(self, cmd):
        try:
            location = sp.check_output('which {}'.format(cmd), shell=True)
        except sp.CalledProcessError as e:
            if e.returncode == 1:
                raise ArcanaRequirementNotFoundError(
                    "Could not find version of {} in environment (using {})"
                    .format(self, self._test_cmd))
            else:
                raise
        if PY3:
            location = location.decode('utf-8')
        location = location.strip()
        return location
