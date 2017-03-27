import re
from itertools import izip_longest
from nianalysis.exceptions import (
    NiAnalysisError, NiAnalysisRequirementVersionException)


def split_version(version_str):
    try:
        return tuple(int(p) for p in version_str.split('.'))
    except ValueError as e:
        raise NiAnalysisRequirementVersionException(str(e))


def matlab_version_split(version_str):
    match = re.match(r'(\d+)(\w)', version_str)
    if match is None:
        raise NiAnalysisRequirementVersionException(
            "Do not understand Matlab version '{}'".format(version_str))
    return match.group(1), match.group(2)


def date_split(version_str):
    return tuple(int(p) for p in version_str.split('-'))


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
                 version_split=split_version):
        self._name = name
        self._min_ver = tuple(min_version)
        if max_version is not None:
            self._max_ver = tuple(max_version)
            if not self.later_or_equal_version(self._max_ver, self._min_ver):
                raise NiAnalysisError(
                    "Supplied max version ({}) is not greater than min "
                    " version ({})".format(self._min_ver, self._max_ver))
        else:
            self._max_ver = None
        self._version_split = version_split

    @property
    def name(self):
        return self._name

    @property
    def min_version(self):
        return self._min_version

    @property
    def max_version(self):
        return self._max_version

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
                v_parts = list(self._version_split(ver))
            except NiAnalysisRequirementVersionException:
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
            raise NiAnalysisRequirementVersionException(msg)
        return best[0]

    def valid_version(self, version):
        return (self.later_or_equal_version(version, self.min_version) and
                (self.max_version is None or
                 self.later_or_equal_version(self.max_version, version)))

    @classmethod
    def later_or_equal_version(cls, version, reference):
        for v_part, r_part in izip_longest(version, reference, fillvalue=0):
            assert isinstance(v_part, type(r_part))
            if v_part > r_part:
                return True
            elif v_part < r_part:
                return False
        return True

mrtrix3_req = Requirement('mrtrix', min_version=(0, 3, 12))

fsl5_req = Requirement('fsl', min_version=(5, 0, 8))

ants2_req = Requirement('ants', min_version=(2, 0))

spm12_req = Requirement('spm', min_version=(12, 0))

freesurfer_req = Requirement('freesurfer', min_version=(5, 3))

matlab2014_req = Requirement('matlab', min_version=(2014, 'a'),
                             version_split=matlab_version_split)

matlab2016_req = Requirement('matlab', min_version=(2016, 'a'),
                             version_split=matlab_version_split)

noddi_req = Requirement('noddi', min_version=(0, 9)),

niftimatlab_req = Requirement('niftimatlib', (1, 2))

dcm2niix_req = Requirement('dcm2niix', min_version=(2017, 2, 7),
                           version_split=date_split)
