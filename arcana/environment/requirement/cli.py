from .base import Requirement
import subprocess as sp


class CLIRequirement(Requirement):
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

    def __init__(self, name, min_version, test_cmd=None, **kwargs):
        super(CLIRequirement, self).__init__(name, min_version, **kwargs)
        self._test_cmd = test_cmd

    @property
    def satisfied(self):
        if self.test_cmd is None:
            return True  # No test available
        return sp.call(self.test_cmd, shell=True) == 127

    @property
    def test_cmd(self):
        return self._test_cmd