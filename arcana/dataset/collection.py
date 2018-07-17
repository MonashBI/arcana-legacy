from arcana.exception import (
    ArcanaError, ArcanaUsageError, ArcanaIndexError)
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
        if collection:
            self._repository = self._common_attr(collection,
                                                 'repository')
            self._from_study = self._common_attr(collection,
                                                 'from_study')
        else:
            self._repository = None
            self._from_study = None
        if frequency == 'per_project':
            # If wrapped in an iterable
            if not isinstance(collection, self.CollectedClass):
                if len(collection) > 1:
                    raise ArcanaUsageError(
                        "More than one {} passed to {}"
                        .format(self.CONTAINED_CLASS.__name__,
                                type(self).__name__))
                collection = list(collection)
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
            return iter(self._collection)
        elif self._frequency == 'per_session':
            return chain(*(c.values()
                           for c in self._collection.values()))
        else:
            return iter(self._collection.values())

    def __len__(self):
        return len(self._collection)

    @property
    def repository(self):
        return self._repository

    @property
    def from_study(self):
        return self._from_study

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
            try:
                subj_dct = self._collection[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' collection ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._collection.keys())))
            try:
                dataset = subj_dct[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in subject {} of '{}' "
                    "collection ({})"
                    .format(visit_id, subject_id, self.name,
                            ', '.join(subj_dct.keys())))
        elif self.frequency == 'per_subject':
            if subject_id is None:
                raise ArcanaError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                dataset = self._collection[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' collection ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._collection.keys())))
        elif self.frequency == 'per_visit':
            if visit_id is None:
                raise ArcanaError(
                    "The 'visit_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                dataset = self._collection[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in '{}' collection ({})"
                    .format(visit_id, self.name,
                            ', '.join(self._collection.keys())))
        elif self.frequency == 'per_project':
            try:
                dataset = self._collection[0]
            except IndexError:
                raise ArcanaIndexError(
                    "'{}' Collection is empty so doesn't have a "
                    "per_project node".format(self.name))
        return dataset

    @property
    def collection(self):
        "Used for duck typing Collection objects with Spec and Match "
        "in source and sink initiation"
        return self

    def bind(self, study):
        "Used for duck typing Collection objects with Spec and Match "
        "in source and sink initiation"
        pass


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

    def __init__(self, name, collection, frequency=None,
                 format=None):  # @ReservedAssignment
        collection = list(collection)
        if collection:
            implicit_frequency = self._common_attr(collection,
                                                   'frequency')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for '{}' DatasetCollection"
                    .format(implicit_frequency, frequency, name))
            implicit_format = self._common_attr(collection, 'format')
            if format is None:
                format = implicit_format  # @ReservedAssignment
            elif format != implicit_format:
                raise ArcanaUsageError(
                    "Implicit format '{}' does not match explicit "
                    "format '{}' for '{}' DatasetCollection"
                    .format(implicit_format, format, name))
        if frequency is None:
            raise ArcanaUsageError(
                "Need to provide explicit frequency for empty "
                "DatasetCollection")
        if format is None:
            raise ArcanaUsageError(
                "Need to provide explicit format for empty "
                "DatasetCollection")
        BaseDataset.__init__(self, name, format, frequency=frequency)
        BaseCollection.__init__(self, collection, frequency)

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

    def __init__(self, name, collection, frequency=None, dtype=None):
        collection = list(collection)
        if collection:
            implicit_frequency = self._common_attr(collection,
                                                   'frequency')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for '{}' DatasetCollection"
                    .dtype(implicit_frequency, frequency, name))
            implicit_dtype = self._common_attr(collection, 'dtype')
            if dtype is None:
                dtype = implicit_dtype  # @ReservedAssignment
            elif dtype != implicit_dtype:
                raise ArcanaUsageError(
                    "Implicit dtype '{}' does not match explicit "
                    "dtype '{}' for '{}' DatasetCollection"
                    .dtype(implicit_dtype, dtype, name))
        if frequency is None:
            raise ArcanaUsageError(
                "Need to provide explicit frequency for empty "
                "DatasetCollection")
        if dtype is None:
            raise ArcanaUsageError(
                "Need to provide explicit dtype for empty "
                "DatasetCollection")
        BaseField.__init__(self, name, dtype=dtype, frequency=frequency)
        BaseCollection.__init__(self, collection, frequency)
