from arcana.exception import ArcanaError, ArcanaUsageError
from .base import BaseDataset, BaseField
from .item import Dataset, Field
from collections import OrderedDict
from operator import itemgetter
from itertools import chain


DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class BaseCollection(object):
    """
    Base class for collection of datasets and field items
    """

    def __init__(self, collection, frequency):
        self._frequency = frequency
        self._repository = self._common_attr(collection, 'repository')
        if frequency == 'per_project':
            # If wrapped in an iterable
            if not isinstance(collection, self.CollectedClass):
                if len(collection) != 1:
                    raise ArcanaUsageError(
                        "More than one {} passed to {}"
                        .format(self.CONTAINED_CLASS.__name__,
                                type(self).__name__))
                collection = list(collection)[0]
            self._collection = collection
        elif frequency == 'per_session':
            self._collection = OrderedDict()
            for subj_id in sorted(set(c.subject_id for c in collection)):
                self._collection[subj_id] = OrderedDict(
                    sorted(((c.visit_id, c) for c in collection
                            if c.subject_id == subj_id),
                           key=itemgetter(0)))
        elif frequency == 'per_subject':
            self._collection = OrderedDict(
                sorted(((c.subject_id, c) for c in collection),
                       key=itemgetter(0)))
        elif frequency == 'per_visit':
            self._collection = OrderedDict(
                sorted(((c.visit_id, c) for c in collection),
                       key=itemgetter(0)))
        else:
            assert False
        for datum in self:
            if not isinstance(datum, self.CollectedClass):
                raise ArcanaUsageError(
                    "Invalid class {} in {}".format(datum, self))

    def __iter__(self):
        if self._frequency == 'per_project':
            return iter((self._collection,))
        elif self._frequency == 'per_session':
            return chain(*(c.values()
                           for c in self._collection.values()))
        else:
            return iter(self._collection.values())

    @property
    def repository(self):
        return self._repository

    @classmethod
    def _common_attr(self, collection, attr_name):
        attr_set = set(getattr(c, attr_name) for c in collection)
        if len(attr_set) != 1:
            raise ArcanaUsageError(
                "Heterogeneous attributes for '{}' within {}".format(
                    attr_name, self))
        return next(iter(attr_set))

    def item(self, subject_id=None, visit_id=None):
        """
        Returns a particular dataset|field in the collection corresponding to
        the given subject and visit_ids. subject_id and visit_id must be
        provided for relevant frequencies. Note that subject_id/visit_id can
        also be provided for non-relevant frequencies, they will just be
        ignored.

        Parameter
        ---------
        subject_id : str
            The subject id of the item to return
        visit_id : str
            The visit id of the item to return
        """

        if self.frequency == 'per_session':
            if subject_id is None or visit_id is None:
                raise ArcanaError(
                    "The 'subject_id' and 'visit_id' must be provided "
                    "to get the match from {}".format(self))
            dataset = self._collection[subject_id][visit_id]
        elif self.frequency == 'per_subject':
            if subject_id is None:
                raise ArcanaError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            dataset = self._collection[subject_id]
        elif self.frequency == 'per_visit':
            if visit_id is None:
                raise ArcanaError(
                    "The 'visit_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            dataset = self._collection[visit_id]
        elif self.frequency == 'per_project':
            dataset = self._collection
        return dataset


class DatasetCollection(BaseCollection, BaseDataset):
    """
    A collection of equivalent datasets (either within a repository)

    Parameters
    ----------
    name : str
        Name of the collection
    collection : List[Dataset]
        An iterable of equivalent datasets
    """

    CollectedClass = Dataset

    def __init__(self, name, collection):
        collection = list(collection)
        if not collection:
            raise ArcanaError(
                "DatasetCollection '{}' cannot be empty".format(name))
        frequency = self._common_attr(collection, 'frequency')
        BaseCollection.__init__(self, collection, frequency)
        BaseDataset.__init__(
            self, name, format=self._common_attr(collection, 'format'),
            frequency=frequency)

    def path(self, subject_id=None, visit_id=None):
        return self.item(
            subject_id=subject_id, visit_id=visit_id).path


class FieldCollection(BaseCollection, BaseField):
    """
    A collection of equivalent datasets (either within a repository)

    Parameters
    ----------
    name : str
        Name of the collection
    collection : List[Dataset]
        An iterable of equivalent datasets
    """

    CollectedClass = Field

    def __init__(self, name, collection):
        collection = list(collection)
        if not collection:
            raise ArcanaError(
                "FieldCollection '{}' cannot be empty".format(name))
        frequency = self._common_attr(collection, 'frequency')
        BaseCollection.__init__(self, collection, frequency)
        BaseField.__init__(
            self, name, dtype=self._common_attr(collection, 'dtype'),
            frequency=frequency)
