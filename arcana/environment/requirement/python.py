import importlib
from .base import Requirement
from arcana.exception import (
    ArcanaRequirementNotFoundError,
    ArcanaRequirementVersionNotDectableError)


class PythonPackageRequirement(Requirement):

    def __init__(self, name, package_name=None, version_attr='__version__',
                 **kwargs):
        super(PythonPackageRequirement, self).__init__(name, **kwargs)
        self._package_name = package_name if package_name is not None else name
        self._version_attr = version_attr

    @property
    def package_name(self):
        return self._package_name

    @property
    def version_attr(self):
        return self._version_attr

    def detect_version(self):
        try:
            module = importlib.import_module(self.package_name)
        except ModuleNotFoundError:
            raise ArcanaRequirementNotFoundError(
                "Did not find '{}' module/package for {}"
                .format(self.package_name, self))
        if self.version_attr is None:
            raise ArcanaRequirementVersionNotDectableError(
                "Could not detect version of {} as version information is not "
                "provided in package".format(self))
        try:
            version_str = getattr(module, self.version_attr)
        except KeyError:
            raise ArcanaRequirementVersionNotDectableError(
                "Version attribute '{}' is not present in loaded version of {}"
                " ({})".format(self.version_attr, self, self.package_name))
        return self.parse_version(version_str)
