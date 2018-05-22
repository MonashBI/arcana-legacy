import re
from itertools import izip_longest
from arcana.exception import (
    ArcanaError, ArcanaRequirementVersionException)
import logging

logger = logging.getLogger('Arcana')


def split_version(version_str):
    logger.debug("splitting version string '{}'"
                 .format(version_str))
    try:
        sanitized_ver_str = re.match(r'[^\d]*(\d+(?:\.\d+)*)[^\d]*',
                                     version_str).group(1)
        return tuple(
            int(p) for p in sanitized_ver_str.split('.'))
    except (ValueError, AttributeError) as e:
        raise ArcanaRequirementVersionException(
            "Could not parse version string '{}': {}".format(
                version_str, e))


def date_split(version_str):
    try:
        return tuple(int(p) for p in version_str.split('-'))
    except ValueError as e:
        raise ArcanaRequirementVersionException(str(e))


def matlab_version_split(version_str):
    match = re.match(r'(?:r|R)?(\d+)(\w)', version_str)
    if match is None:
        raise ArcanaRequirementVersionException(
            "Do not understand Matlab version '{}'".format(version_str))
    return int(match.group(1)), match.group(2).lower()


class Requirement(object):
    """
    Defines a software package that is required by a processing Node, which is
    typically wrapped up in an environment module (see
    http://modules.sourceforge.net)

    Parameters
    ----------
    name : str
        Name of the package
    min_version : tuple(int|str)
        The minimum version required by the node
    max_version : tuple(int|str) | None
        The maximum version that is compatible with the Node
    """

    def __init__(self, name, min_version, max_version=None,
                 version_split=split_version, citations=None):
        self._name = name.lower()
        self._min_ver = tuple(min_version)
        if max_version is not None:
            self._max_ver = tuple(max_version)
            if not self.later_or_equal_version(self._max_ver, self._min_ver):
                raise ArcanaError(
                    "Supplied max version ({}) is not greater than min "
                    " version ({})".format(self._min_ver, self._max_ver))
        else:
            self._max_ver = None
        self._version_split = version_split
        self._citations = citations if citations is not None else []

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return "{} (v >= {}{})".format(
            self.name, self.min_version,
            (", v <= {}".format(self.max_version)
             if self.max_version is not None else ''))

    @property
    def min_version(self):
        return self._min_ver

    @property
    def citations(self):
        return iter(self._citations)

    @property
    def max_version(self):
        return self._max_ver

    def split_version(self, version_str):
        return tuple(self._version_split(version_str))

    def best_version(self, available_versions):
        """
        Picks the latest acceptible version from the versions available

        Parameters
        ----------
        available_versions : list(str)
            List of possible versions
        """
        best = None
        for ver in available_versions:
            try:
                v_parts = self.split_version(ver)
            except ArcanaRequirementVersionException:
                continue  # Incompatible version
            if (self.later_or_equal_version(v_parts, self._min_ver) and
                (self._max_ver is None or
                 self.later_or_equal_version(self._max_ver, v_parts))):
                if best is None or self.later_or_equal_version(v_parts,
                                                               best[1]):
                    best = ver, v_parts
        if best is None:
            msg = ("Could not find version of '{}' matching requirements "
                   "> ({})"
                   .format(self.name,
                           ', '.join(str(v) for v in self._min_ver)))
            if self._max_ver is not None:
                msg += " and < ({})".format(
                    ', '.join(str(v) for v in self._max_ver))
            msg += " from available versions '{}'".format(
                "', '".join(available_versions))
            raise ArcanaRequirementVersionException(msg)
        return best[0]

    def valid_version(self, version):
        return (self.later_or_equal_version(version, self.min_version) and
                (self.max_version is None or
                 self.later_or_equal_version(self.max_version, version)))

    @classmethod
    def later_or_equal_version(cls, version, reference):
        for v_part, r_part in izip_longest(version, reference, fillvalue=0):
            if type(v_part) != type(r_part):
                raise ArcanaError(
                    "Type of version part {} (of '{}'), {}, does not match "
                    "type of reference part {}, {}".format(
                        v_part, version, type(v_part), r_part, type(r_part)))
            if v_part > r_part:
                return True
            elif v_part < r_part:
                return False
        return True

    @classmethod
    def best_requirement(cls, possible_requirements, available_modules,
                         preloaded_modules=None):
        if preloaded_modules is None:
            preloaded_modules = {}
        # If possible reqs is a singleton, wrap it in a list for
        # iterating
        if isinstance(possible_requirements, Requirement):
            possible_requirements = [possible_requirements]
        # Loop through all options for a given requirement and see
        # if at least one can be satisfied.
        logger.debug(
            "Searching for one of {}".format(
                ', '.join(str(r) for r in possible_requirements)))
        ver_exceptions = []  # Will hold all version error messages
        for req in possible_requirements:
            try:
                version = preloaded_modules[req.name]
                logger.debug("Found preloaded version {} of module '{}'"
                             .format(version, req.name))
                if req.valid_version(req.split_version(version)):
                    return req.name, version
                else:
                    raise ArcanaError(
                        "Incompatible module version already loaded {}/{},"
                        " (valid {}->{}) please unload before running "
                        "pipeline"
                        .format(
                            req.name, version, req.min_version,
                            (req.max_version if req.max_version is not None
                             else '')))
            except KeyError:
                try:
                    best_version = req.best_version(
                        available_modules[req.name])
                    logger.debug("Found best version '{}' of module '{}' for"
                                 " requirement {}".format(best_version,
                                                          req.name, req))
                    return req.name, best_version
                except ArcanaRequirementVersionException as e:
                    ver_exceptions.append(e)
        # If no options can be satisfied, otherwise raise exception with
        # combined messages from all options.
        raise ArcanaRequirementVersionException(
            ' and '.join(str(e) for e in ver_exceptions))
