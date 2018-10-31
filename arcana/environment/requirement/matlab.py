from past.builtins import basestring
import shutil
import tempfile
from .base import Requirement, Version, VersionRange
from nipype.interfaces.matlab import MatlabCommand
from arcana.exception import (
    ArcanaVersionNotDectableError, ArcanaRequirementNotFoundError)
from .cli import CliRequirement
import re


def run_matlab_cmd(cmd):
    delim = '????????'  # A string that won't occur in the Matlab splash
    matlab_cmd = MatlabCommand(
        script=("fprintf('{}'); fprintf({}); exit;".format(delim, cmd)))
    tmp_dir = tempfile.mkdtemp()
    try:
        result = matlab_cmd.run(cwd=tmp_dir)
        return result.runtime.stdout.split(delim)[1]
    finally:
        shutil.rmtree(tmp_dir)


class MatlabRequirement(CliRequirement):

    def __init__(self):
        super().__init__('matlab', 'matlab', version_switch=None)

    def detect_version(self):
        """
        Finds the version of the software requirement that is accessible in
        the current environment. Should be overridden in sub-classes
        """
        # The matlab version should be included in the opening splash
        return self.parse_version(run_matlab_cmd("version('-release')"))

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
        match = re.search(r'(?:R)?(\d+)(a|b)', version_str,
                          re.IGNORECASE)  # @UndefinedVariable
        if match is None:
            raise ArcanaVersionNotDectableError(
                "Could not parse Matlab version string '{}'"
                .format(version_str))
        return int(match.group(1)), match.group(2).lower()

    def format_version(self, version_tuple):
        return 'R{}{}'.format(*version_tuple)


matlab_req = MatlabRequirement()


class MatlabPackageVersion(Version):

    def __init__(self, requirement, version, matlab_version):
        super(MatlabPackageVersion, self).__init__(requirement,
                                                              version)
        if isinstance(matlab_version, basestring):
            matlab_version = matlab_req.v(matlab_version)
        elif isinstance(matlab_version, (tuple, list)):
            matlab_version = matlab_req.v(*matlab_version)
        self._matlab_version = matlab_version

    def __eq__(self, other):
        return (
            super(MatlabPackageVersion, self).__eq__(other) and
            self._matlab_version == other._matlab_version)

    @property
    def matlab_version(self):
        return self._matlab_version


class MatlabPackageVersionRange(VersionRange):

    VersionClass = MatlabPackageVersion

    def __init__(self, requirement, min_version, max_version, matlab_version):
        super(MatlabPackageVersionRange, self).__init__(
            requirement, min_version, max_version)
        if isinstance(matlab_version, basestring):
            matlab_version = matlab_req.v(matlab_version)
        elif isinstance(matlab_version, (tuple, list)):
            matlab_version = matlab_req.v(*matlab_version)
        self._matlab_version = matlab_version

    def __eq__(self, other):
        return (
            super(MatlabPackageVersionRange, self).__eq__(other) and
            self._matlab_version == other._matlab_version)

    @property
    def matlab_version(self):
        return self._matlab_version

    @property
    def _version_kwargs(self):
        return {'matlab_version': self.matlab_version}


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
    """

    VersionRangeClass = MatlabPackageVersionRange

    def __init__(self, name, test_func, **kwargs):
        super(MatlabPackageRequirement, self).__init__(name, **kwargs)
        self._test_func = test_func

    def __eq__(self, other):
        return (super(MatlabPackageRequirement, self).__eq__(other) and
                self._test_func == other._test_func)

    @property
    def test_func(self):
        return self._test_func

    def detect_version(self):
        help_text = run_matlab_cmd("help('{}')".format(self.test_func))
        if not help_text:
            raise ArcanaRequirementNotFoundError(
                "Did not find test function '{}' for {}"
                .format(self.test_func, self))
        return self.parse_help_text(help_text)

    def parse_help_text(self, help_text):
        return self.parse_version(help_text)
