from abc import ABCMeta, abstractmethod


class Archive(object):
    """
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def source(self):
        pass

    @abstractmethod
    def sink(self):
        pass

    @abstractmethod
    def all_sessions(self, project_id, study_id=None):
        pass

    @abstractmethod
    def sessions_with_dataset(self, file_, project_id, sessions=None):
        pass
