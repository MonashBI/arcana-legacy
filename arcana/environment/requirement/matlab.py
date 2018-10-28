from .base import Requirement
from nipype.interfaces.matlab import MatlabCommand
from arcana.exception import (
    ArcanaRequirementVersionException)
import re


def matlab_version_split(version_str):
    match = re.match(r'(?:r|R)?(\d+)(\w)', version_str)
    if match is None:
        raise ArcanaRequirementVersionException(
            "Do not understand Matlab version '{}'".format(version_str))
    return int(match.group(1)), match.group(2).lower()


class MatlabRequirement(Requirement):
    """
    Defines a software package within Matlab

    Parameters
    ----------
    name : str
        Name of the package
    min_version : tuple(int|str)
        The minimum version required by the node
    max_version : tuple(int|str) | None
        The maximum version that is compatible with the Node
    test_func : str
        The name of a function that should be available when the
        requirement is satisfied.
    version_split : function
        A function that splits the version string into major/minor/micro
        parts or equivalent
    references : list[Citation]
        A list of references that should be cited when using this software
        requirement
    """

    def __init__(self, name, min_version, test_func=None, **kwargs):
        super(MatlabRequirement, self).__init__(name, min_version, **kwargs)
        self._test_func = test_func

    @property
    def satisfied(self):
        if self.test_func is None:
            return True  # No test available
        script = (
            "try\n"
            "    {}\n"
            "catch E\n"
            "    fprintf(E.identifier);\n"
            "end\n".format(self.test_func))
        result = MatlabCommand(script=script, mfile=True).run()
        output = result.runtime.stdout
        return output != 'MATLAB:UndefinedFunction'

    @property
    def test_func(self):
        return self._test_func
