from __future__ import division
from builtins import object
from past.builtins import basestring
import logging
from arcana.exception import (
    ArcanaUsageError, ArcanaRequirementVersionNotDectableError,
    ArcanaRequirementVersionException)
import re


logger = logging.getLogger('arcana')


class RequirementVersion(object):

    def __init__(self, requirement, version):
        self._req = requirement
        if isinstance(version, basestring):
            version = requirement.parse_version(version)
        self._ver = version

    def __repr__(self):
        return "{}[{}]".format(self._req, self.version)

    def __eq__(self, other):
        return self._req == other._req and self._ver == other._ver

    @property
    def version(self):
        return self._req.format_version(self._ver)

    @property
    def requirement(self):
        return self._req


class RequirementVersionRange(object):
    """
    A range of versions associated with a software requirement

    Parameters
    ----------
    requirement : Requirement
        The requirement to define the version range for
    min_version : tuple(int|str)
        The minimum version required by the node
    max_version : tuple(int|str) | None
        The maximum version that is compatible with the Node
    """

    VersionClass = RequirementVersion
    _version_kwargs = {}

    def __init__(self, requirement, min_version, max_version):
        self._req = requirement
        if isinstance(min_version, basestring):
            min_version = self.parse_version(min_version)
        if isinstance(max_version, basestring):
            max_version = self.parse_version(max_version)
        if max_version < min_version:
            raise ArcanaUsageError(
                "Maxium version in {} version range {} is less than minimum {}"
                .format(self._req.name, max_version, min_version))
        self._min_ver = min_version
        self._max_ver = max_version

    def __eq__(self, other):
        return (self._req == other._req and self._min_ver == other._min_ver and
                self._max_ver == other._max_ver)

    @property
    def min_version(self):
        return self._req.format_version(self._min_ver)

    @property
    def max_version(self):
        return self._req.format_version(self._max_ver)

    def __repr__(self):
        return "{}[{} <= v <= {}]".format(
            self._req, self.min_version, self.max_version)

    def __contains__(self, version):
        return version >= self._min_ver and version <= self._max_ver

    def latest_available(self, available, ignore_unrecognised=False):
        """
        Picks the latest acceptible version from the versions available

        Parameters
        ----------
        available : list(tuple(int) | str)
            List of possible versions to select from
        ignore_unrecognised : bool
            If True, then unrecognisable versions are ignored instead of
            throwing an error

        Returns
        -------
        latest : RequirementVersion
            The latest version
        """
        latest_ver = ()
        for ver in available:
            if isinstance(ver, basestring):
                try:
                    ver = self._req.parse_version(ver)
                except ArcanaRequirementVersionNotDectableError:
                    if ignore_unrecognised:
                        continue
                    else:
                        raise
            if ver < self._min_ver:
                logger.debug("Not selecting {} version of {} as it is not in "
                             "acceptable range {} <= v <= {} ".format(
                                 ver, self._req, self.min_version,
                                 self.max_version))
            elif ver > self._max_ver:
                logger.info("Not selecting {} version of {} as it is not in "
                            "acceptable range {} <= v <= {} ".format(
                                ver, self._req, self.min_version,
                                self.max_version))
            elif ver > latest_ver:
                latest_ver = ver
        if not latest_ver:
            raise ArcanaRequirementVersionException(
                "Could not find version of {} within range {} <= v <= {} "
                "(available {})".format(self._req, self.min_version,
                                        self.max_version,
                                        ', '.join(str(v) for v in available)))
        return self.VersionClass(self._req, latest_ver, **self._version_kwargs)


class Requirement(object):
    """
    Base class for a details of a software package that is required by
    a node of a pipeline.

    Parameters
    ----------
    name : str
        Name of the package
    references : list[Citation]
        A list of references that should be cited when using this software
        requirement
    website : str
        Address of the website detailing the software
    delimeter : str
        Delimeter used to split a version string
    """

    VersionRangeClass = RequirementVersionRange

    def __init__(self, name, references=None, website=None, delimeter='.',
                 version_regex=None):
        self._name = name.lower()
        self._references = references if references is not None else []
        self._website = website
        if len(delimeter) > 1:
            raise ArcanaUsageError(
                "Only single character delimeters are allowed ('{}' provided)"
                .format(delimeter))
        self._delim = delimeter
        if version_regex is None:
            # Escape delimeter if required
            m = ('\\' + delimeter if delimeter in r'{}\$.|?*+()[]'
                 else delimeter)
            # Pattern to match sub-version
            sub_ver = r'{}\d+[a-zA-Z\-_0-9]*'.format(m)
            version_regex = (
                r'(?<!\d{m})(\d+{sv}(?:{sv})?(?:{m}\w+)?)'
                .format(m=m, sv=sub_ver))
        self._version_regex = re.compile(version_regex)

    def __eq__(self, other):
        return (self.name == other.name and
                self._references == other._references and
                self.website == other.website and
                self._delim == other._delim and
                self._version_regex == other._version_regex)

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return "{}(name={})".format(type(self).__name__, self.name)

    def v(self, version, max_version=None, **kwargs):
        """
        Returns either a single requirement version or a requirement version
        range depending on whether two arguments are supplied or one

        Parameters
        ----------
        version : str | RequirementVersion
            Either a version of the requirement, or the first version in a
            range of acceptable versions
        """
        if max_version is None:
            req_ver = self.VersionRangeClass.VersionClass(self, version,
                                                          **kwargs)
        else:
            req_ver = self.VersionRangeClass(self, version, max_version,
                                             **kwargs)
        return req_ver

    @property
    def references(self):
        return iter(self._references)

    @property
    def website(self):
        return self._website

    @property
    def delimeter(self):
        return self._delim

    @property
    def version_regex(self):
        return self._version_regex

    def parse_version(self, version_str):
        """
        Splits a typical version string (e.g. <MAJOR>.<MINOR>.<MICRO>)
        into a tuple that can be sorted properly. Ignores all leading and
        trailing characters.

        Parameters
        ----------
        version_str : str
            The string containing the version numbers

        Returns
        -------
        version : tuple(int | str)
            A tuple containing the major, minor and micro (if provided)
            version numbers.
        """
        match = self.version_regex.search(version_str)
        if match is None:
            raise ArcanaRequirementVersionNotDectableError(
                "Could not parse version string for {}:\n{}"
                .format(self, version_str))
        version = []
        for part in match.group(1).split(self.delimeter):
            # Split on non-numeric parts of the version string so that we
            # can convert the numeric parts to ints
            for sub_part in re.split('([^\d]+)', part):
                if sub_part:
                    try:
                        sub_part = int(sub_part)
                    except ValueError:
                        pass
                    version.append(sub_part)
        return tuple(version)

    def detect_version(self):
        """
        Finds the version of the software requirement that is accessible in
        the current environment. Should be overridden in sub-classes
        """
        raise NotImplementedError

    def format_version(self, version_tuple):
        return self.delimeter.join(str(i) for i in version_tuple)
