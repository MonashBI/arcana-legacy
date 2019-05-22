import importlib
from .base import BaseRequirement
from arcana.exceptions import (
    ArcanaRequirementNotFoundError,
    ArcanaVersionNotDetectableError)


class PythonPackageRequirement(BaseRequirement):

    def __init__(self, name, package_name=None, version_attr='__version__',
                 **kwargs):
        super(PythonPackageRequirement, self).__init__(name, **kwargs)
        self._package_name = package_name if package_name is not None else name
        self._version_attr = version_attr

    def __eq__(self, other):
        return (super(PythonPackageRequirement, self).__eq__(other) and
                self._package_name == other._package_name and
                self._version_attr == other._version_attr)

    def __hash__(self):
        return (super().__hash__() ^ hash(self._package_name) ^
                hash(self._version_attr))

    @property
    def package_name(self):
        return self._package_name

    @property
    def version_attr(self):
        return self._version_attr

    def detect_version_str(self):
        try:
            module = importlib.import_module(self.package_name)
        except ImportError:
            raise ArcanaRequirementNotFoundError(
                "Did not find '{}' module/package for {}"
                .format(self.package_name, self))
        if self.version_attr is None:
            raise ArcanaVersionNotDetectableError(
                "Could not detect version of {} as version information is not "
                "provided in package".format(self))
        try:
            version_str = getattr(module, self.version_attr)
        except KeyError:
            raise ArcanaVersionNotDetectableError(
                "Version attribute '{}' is not present in loaded version of {}"
                " ({})".format(self.version_attr, self, self.package_name))
        return version_str
