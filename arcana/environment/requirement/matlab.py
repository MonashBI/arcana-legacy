import re
from .base import BaseRequirement, Version
from arcana.utils import run_matlab_cmd
from arcana.exceptions import (
    ArcanaVersionNotDetectableError, ArcanaRequirementNotFoundError)


class MatlabVersion(Version):
    """
    A representation of the Matlab release, e.g. R2017b
    """

    @classmethod
    def parse(cls, version_str):
        regex = r'R?(\d+)(a|b)'
        match = re.search(regex, version_str, re.IGNORECASE)
        if match is None:
            raise ArcanaVersionNotDetectableError(
                "Could not parse Matlab version string {} as {}. Regex ({})"
                " did not match any sub-string".format(
                    version_str, cls.__name__, regex))
        return (int(match.group(1)), match.group(2).lower()), None, None, None

    def __str__(self):
        return 'R{}{}'.format(*self.sequence)


class MatlabRequirement(BaseRequirement):

    def detect_version_str(self):
        """
        Finds the version of Matlab requirement that is accessible in
        the current environment.
        """
        # The matlab version should be included in the opening splash
        return run_matlab_cmd("version('-release')")


matlab_req = MatlabRequirement(
    'matlab', version_cls=MatlabVersion)


class MatlabPackageRequirement(BaseRequirement):
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

    def __init__(self, name, test_func, **kwargs):
        super(MatlabPackageRequirement, self).__init__(name, **kwargs)
        self._test_func = test_func

    def __eq__(self, other):
        return (super(MatlabPackageRequirement, self).__eq__(other) and
                self._test_func == other._test_func)

    def __hash__(self):
        return super().__hash__() ^ hash(self._test_func)

    @property
    def test_func(self):
        return self._test_func

    def detect_version_str(self):
        """
        Try to detect version of package from command help text. Bit of a long
        shot as they are typically included
        """
        help_text = run_matlab_cmd("help('{}')".format(self.test_func))
        if not help_text:
            raise ArcanaRequirementNotFoundError(
                "Did not find test function '{}' for {}"
                .format(self.test_func, self))
        return self.parse_help_text(help_text)

    def parse_help_text(self, help_text):
        return help_text
