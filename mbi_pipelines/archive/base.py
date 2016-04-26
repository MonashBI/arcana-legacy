from abc import ABCMeta, abstractmethod


class Archive(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def source(self):
        pass

    @abstractmethod
    def sink(self):
        pass

    @abstractmethod
    def subject_ids(self):
        pass

    @abstractmethod
    def sessions_with_dataset(self, dataset, sessions=None):
        pass
