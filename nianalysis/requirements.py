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

    @property
    def module_name(self):
        return self._name


mrtrix3_req = Requirement('mrtrix', min_version=(0, 3, 12))

fsl5_req = Requirement('fsl', min_version=(5, 0, 8))

ants2_req = Requirement('ants', min_version=(2, 0))

spm12_req = Requirement('spm', min_version=(12, 0))

freesurfer_req = Requirement('freesurfer', min_version=(5, 3))

matlab_req = Requirement('matlab', min_version=(2014, 'a'))
