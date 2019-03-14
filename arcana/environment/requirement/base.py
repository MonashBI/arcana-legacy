from __future__ import division
from builtins import object
import math
import logging
from arcana.exceptions import (
    ArcanaUsageError, ArcanaVersionNotDetectableError, ArcanaVersionError)
import re


logger = logging.getLogger('arcana')


class Version(object):
    """
    Representation of a requirement version. Parses version strings that
    follow the convention

    i.e. <MACRO>.<MINOR>[<PRERELEASE>].[<MICRO>[<PRERELEASE>]][.dev<REVISION>]

    Parameters
    ----------
    requirement : Requirement
        The requirement the version is of
    version : str
        The string representation of the version
    local_name : str
        The name of the requirement as referred to in the local environment
    local_version : str
        The version str as referred to in the local environment
    """

    delimeter = '.'

    def __init__(self, requirement, version, local_name=None,
                 local_version=None):
        self._req = requirement
        self._seq, self._prerelease, self._post, self._dev = self.parse(
            version)
        self._local_version = (
            local_version if local_version != str(self) else None)
        self._local_name = (
            local_name if local_name != requirement.name else None)

    @property
    def requirement(self):
        return self._req

    @property
    def name(self):
        return self.requirement.name

    @property
    def sequence(self):
        return self._seq

    @property
    def prerelease(self):
        return self._prerelease

    @property
    def dev(self):
        return self._dev

    @property
    def post(self):
        return self._post

    @property
    def local_version(self):
        """
        The un-parsed version string. Used to cross-reference with list of
        available versions coming from an Environment
        """
        return (self._local_version
                if self._local_version is not None else str(self))

    @property
    def local_name(self):
        return (self._local_name
                if self._local_name is not None else self.requirement.name)

    def serialise(self):
        pass

    @classmethod
    def unserialise(cls, dct):
        pass

    def __str__(self):
        s = self.delimeter.join(str(i) for i in self._seq)
        if self._prerelease is not None:
            s += '{}{}'.format(*self._prerelease)
        if self._post is not None:
            s += '.post{}'.format(self._post)
        if self._dev is not None:
            s += '.dev{}'.format(self._dev)
        return s

    def __repr__(self):
        return "{}[{}]".format(self._req, str(self))

    def __eq__(self, other):
        return (self._req == other._req and
                self._seq == other._seq and
                self._prerelease == other._prerelease and
                self._dev == other._dev)

    def compare(self, other):
        """
        Compares the version with another

        Parameters
        ----------
        other : Version
            The version to compare to
        """
        if self._req != other._req:
            raise ArcanaUsageError(
                "Can't compare versions of different requirements {} and {}"
                .format(self._req, other._req))
        # Compare main sequence
        if self._seq < other._seq:
            return -1
        elif self._seq > other._seq:
            return 1
        # If main sequence is equal check prerelease. If a prerelease is
        # None then it is a full release which is > then a prerelease so we
        # just assign it 'z' (which is greater than 'a', 'b' and 'rc')
        s = self._prerelease if self._prerelease is not None else ('z',)
        o = other._prerelease if other._prerelease is not None else ('z',)
        if s < o:
            return -1
        if s > o:
            return 1
        # If both main sequence and prereleases are equal, compare post release
        s = self._post if self._post is not None else 0
        o = other._post if other._post is not None else 0
        if s < o:
            return -1
        if s > o:
            return 1
        # If both main sequence and prereleases are equal, compare development
        # release
        s = self._dev if self._dev is not None else 0
        o = other._dev if other._dev is not None else 0
        if s < o:
            return -1
        if s > o:
            return 1
        assert self == other
        return 0

    def __lt__(self, other):
        return self.compare(other) < 0

    def __gt__(self, other):
        return self.compare(other) > 0

    def __le__(self, other):
        return self.compare(other) <= 0

    def __ge__(self, other):
        return self.compare(other) >= 0

    @property
    def regex(self):
        # Escape delimeter if required
        m = ('\\' + self.delimeter if self.delimeter in r'{}\$.|?*+()[]'
             else self.delimeter)
        # Pattern to match sub-version
        sub_ver = r'{}\d+[a-zA-Z\-_0-9]*'.format(m)
        return re.compile(
            r'(?<!\d{m})(\d+{sv}(?:{sv})?(?:{m}\w+)?)'.format(m=m, sv=sub_ver))

    def parse(self, version):
        """
        Splits a typical version string (e.g. <MAJOR>.<MINOR>.<MICRO>)
        into a tuple that can be sorted properly. Ignores all leading
        and trailing characters by using a regex search (instead of match) so
        as to pick the version string out of a block of text.

        Parameters
        ----------
        version : str | int | float | tuple(int)
            The string containing the version numbers, or alternatively an
            integer, float (number after decimal is interpreted as minor ver),
            or tuple|list containing the version sequence.

        Returns
        -------
        sequence : tuple(int | str)
            A tuple containing the main sequence of the version,
            e.g. <MAJOR>.<MINOR>.<MICRO>
        prerelease : 2-tuple(str, int) | None
            A 2-tuple containing the type of prerelease ('a' - alpha,
            'b' - beta, or 'rc' - release-canditate) and the number of the
            prerelease
        post : int |None
            The number of the post version
        dev : int | None
            The number of the development version
        """
        # Check to see if version is not a string but rather another type
        # that can be interpreted as a version
        if isinstance(version, int):
            return (version,), None, None, None
        elif isinstance(version, (tuple, list)):
            return tuple(int(i) for i in version), None, None, None
        elif isinstance(version, float):
            major = math.floor(version)
            minor = version - major
            return (major, minor), None, None, None
        match = self.regex.search(version)
        if match is None:
            raise ArcanaVersionNotDetectableError(
                "Could not parse version string {} as {}. Regex ({}) did not "
                "match any sub-string".format(version, type(self).__name__,
                                              self.regex.pattern))
        sequence = []
        prerelease = None
        dev = None
        post = None
        for part in match.group(1).split(self.delimeter):
            if part.startswith('dev'):
                dev = int(part[len('dev'):])
            elif part.startswith('post'):
                post = int(part[len('post'):])
            else:
                # Split on non-numeric parts of the version string so that we
                # can detect prerelease
                sub_parts = re.split('([^\d]+)', part)
                if sub_parts[0]:
                    try:
                        seq_part = int(sub_parts[0])
                    except ValueError:
                        seq_part = sub_parts[0]
                    sequence.append(seq_part)
                if len(sub_parts) > 1:
                    stage = sub_parts[1]
                    try:
                        pr_ver = int(sub_parts[2])
                    except ValueError:
                        raise ArcanaVersionNotDetectableError(
                            "Could not parse version string {} as {}. "
                            "Did not recognise pre-release version {}"
                            .format(version, type(self).__name__,
                                    sub_parts[2]))
                    stage = stage.strip('-_').lower()
                    if not stage:  # No prerelease info, assume a dev version
                        assert dev is None
                        dev = pr_ver
                        continue
                    if 'alpha'.startswith(stage):
                        stage = 'a'
                    elif 'beta'.startswith(stage):
                        stage = 'b'
                    elif stage == 'rc' or stage == 'release-canditate':
                        stage = 'rc'
                    else:
                        raise ArcanaVersionNotDetectableError(
                            "Could not parse version string {} as {}. "
                            "Did not recognise pre-release stage {}"
                            .format(version, type(self).__name__, stage))
                    prerelease = (stage, pr_ver)
        return tuple(sequence), prerelease, post, dev

    def within(self, version):
        """
        A single version can also be interpreted as an open range (i.e. no
        maximum version)
        """
        if not isinstance(version, Version):
            version = type(self._min_ver)(self._req, version)
        return version >= self

    def latest_within(self, *args, **kwargs):
        return self._req.latest_within_range(self, *args, **kwargs)

    @property
    def prov(self):
        prov = {'version': str(self)}
        if self._local_name is not None:
            prov['local_name'] = self.local_name
        if self._local_version is not None:
            prov['local_version'] = self.local_version
        return prov


class VersionRange(object):
    """
    A range of versions associated with a software requirement

    Parameters
    ----------
    min_version : Version
        The minimum version required by the node
    max_version : Version
        The maximum version that is compatible with the Node
    """

    def __init__(self, min_version, max_version):
        if min_version.requirement != max_version.requirement:
            raise ArcanaUsageError(
                "Inconsistent requirements between min and max versions "
                "({} and {})".format(min_version.requirement,
                                     max_version.requirement))
        self._min_ver = min_version
        self._max_ver = max_version
        if max_version < min_version:
            raise ArcanaUsageError(
                "Maxium version in is less than minimum in {}"
                .format(self))

    @property
    def name(self):
        return self.minimum.name

    @property
    def requirement(self):
        return self.minimum.requirement

    @property
    def minimum(self):
        return self._min_ver

    @property
    def maximum(self):
        return self._max_ver

    def __eq__(self, other):
        return (self._min_ver == other._min_ver and
                self._max_ver == other._max_ver)

    def __str__(self):
        return '{} <= v <= {}'.format(self.minimum, self.maximum)

    def __repr__(self):
        return "{}[{}]".format(
            self._min_ver.requirement, self)

    def within(self, version):
        if not isinstance(version, Version):
            version = type(self._min_ver)(self.requirement, version)
        return version >= self._min_ver and version <= self._max_ver

    def latest_within(self, *args, **kwargs):
        return self._min_ver.requirement.latest_within_range(self, *args,
                                                             **kwargs)


class BaseRequirement(object):
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

    def __init__(self, name, citations=None, website=None,
                 version_cls=Version):
        self._name = name.lower()
        self._citations = citations if citations is not None else []
        self._website = website
        self._version_cls = version_cls

    def __eq__(self, other):
        return (self.name == other.name and self.website == other.website)

    @property
    def name(self):
        return self._name

    @property
    def version_cls(self):
        return self._version_cls

    def __hash__(self):
        return hash(self.name) ^ hash(self.website)

    def __repr__(self):
        return "{}(name={})".format(type(self).__name__, self.name)

    def v(self, version, max_version=None, **kwargs):
        """
        Returns either a single requirement version or a requirement version
        range depending on whether two arguments are supplied or one

        Parameters
        ----------
        version : str | Version
            Either a version of the requirement, or the first version in a
            range of acceptable versions
        """
        if not isinstance(version, Version):
            version = self.version_cls(self, version, **kwargs)
        # Return a version range instead of version
        if max_version is not None:
            if not isinstance(max_version, Version):
                max_version = self.version_cls(self, max_version, **kwargs)
            version = VersionRange(version, max_version)
        return version

    @property
    def citations(self):
        return iter(self._citations)

    @property
    def website(self):
        return self._website

    def detect_version(self, **kwargs):
        return self.version_cls(self, self.detect_version_str(), **kwargs)

    def detect_version_str(self):
        """
        Detects and returns the version string of the software requirement
        that is accessible in the current environment. NB: to be overridden in
        sub-classes.

        * If the requirement is not available in the current environment:
            raise ArcanaRequirementNotFoundError
        * If the requirement is available but its version cannot be detected
          for whatever reason:
            raise ArcanaVersionNotDetectableError
        """
        raise NotImplementedError

    def latest_within_range(self, version_range, available):
        """
        Picks the latest acceptible version from the versions available

        Parameters
        ----------
        version_range : VersionRange | Version
            A range of versions or a single version. A single version
            will be interpreted that there are no upper bounds on the version
            range
        available : list(Version)
            List of possible versions to select from

        Returns
        -------
        latest : Version
            The latest version
        """
        latest_ver = None
        for ver in available:
            if version_range.within(ver) and (latest_ver is None or
                                              ver > latest_ver):
                latest_ver = ver
        if latest_ver is None:
            if isinstance(version_range, VersionRange):
                msg_part = 'within range'
            else:
                msg_part = 'greater than'
            raise ArcanaVersionError(
                "Could not find version {} {} from available: {}"
                .format(msg_part, version_range,
                        ', '.join(str(v) for v in available)))
        return latest_ver
