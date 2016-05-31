class Requirement(object):

    def __init__(self, name, min_version, max_version=None):
        self._name = name
        self._min_version = min_version
        self._max_version = max_version

    @property
    def name(self):
        return self._name

    @property
    def min_version(self):
        return self._min_version

    @property
    def max_version(self):
        return self._max_version


mrtrix3_req = Requirement('mrtrix3', min_version=(0, 3, 12))

fsl5_req = Requirement('fsl', min_version=(5, 0))
