from past.builtins import basestring
from .base import Requirement
from nipype.interfaces.matlab import MatlabCommand
from arcana.exception import (
    ArcanaRequirementVersionException)
from .cli import CliRequirement
import re


class MatlabRequirement(CliRequirement):

    _PRINT_DELIM = '?????'

    def __init__(self):
        super().__init__('matlab', None)

    def detect_version(self):
        """
        Finds the version of the software requirement that is accessible in
        the current environment. Should be overridden in sub-classes
        """
        # The matlab version should be included in the opening splash
        cmd = MatlabCommand(
            script=("fprintf('{}R'); fprintf(version('-release'); exit;"
                    .format(self._PRINT_DELIM)))
        result = cmd.run()
        self.parse_version(result.runtime.stdout.split(self._PRINT_DELIM)[1])

    def parse_version(self, version_str):
        """
        Splits a Matlab version string.

        Parameters
        ----------
        version_str : str
            The string containing the version numbers

        Returns
        -------
        version : tuple(int)
            A tuple containing the major, minor and micro (if provided)
            version numbers.
        """
        match = re.search(r'(?:r|R)(\d+)(a|b)', version_str)
        if match is None:
            raise ArcanaRequirementVersionException(
                "Could not parse Matlab version string '{}'"
                .format(version_str))
        return match.groups()


matlab_req = MatlabRequirement()


class MatlabPackageRequirement(Requirement):
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

    def __init__(self, name, test_func=None, matlab_version=None, **kwargs):
        super(MatlabPackageRequirement, self).__init__(name, **kwargs)
        self._test_func = test_func
        if isinstance(matlab_version, basestring):
            matlab_version = matlab_req(matlab_version)
        self._matlab_version = matlab_version

    @property
    def test_func(self):
        return self._test_func

    @property
    def matlab_version(self):
        return self._matlab_version

    def detect_version(self):
        raise NotImplementedError
