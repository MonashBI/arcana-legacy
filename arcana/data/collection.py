from arcana.exceptions import (
    ArcanaError, ArcanaUsageError, ArcanaIndexError)
from .base import BaseFileset, BaseField
from .item import Fileset, Field
from collections import OrderedDict
from operator import itemgetter
from itertools import chain


DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class BaseCollection(object):
    """
    Base class for collection of filesets and field items
    """

    is_spec = False  # For duck-typing with *Spec and *Selector objects

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
        if frequency == 'per_study':
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
        if self._frequency == 'per_study':
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
        Returns a particular fileset|field in the collection corresponding to
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
                    "The 'subject_id' ({}) and 'visit_id' ({}) must be "
                    "provided to get an item from {}".format(
                        subject_id, visit_id, self))
            try:
                subj_dct = self._collection[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' collection ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._collection.keys())))
            try:
                fileset = subj_dct[visit_id]
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
                fileset = self._collection[subject_id]
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
                fileset = self._collection[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in '{}' collection ({})"
                    .format(visit_id, self.name,
                            ', '.join(self._collection.keys())))
        elif self.frequency == 'per_study':
            try:
                fileset = self._collection[0]
            except IndexError:
                raise ArcanaIndexError(
                    "'{}' Collection is empty so doesn't have a "
                    "per_study node".format(self.name))
        return fileset

    @property
    def collection(self):
        "Used for duck typing Collection objects with Spec and Match "
        "in source and sink initiation"
        return self

    def bind(self, study):
        """
        Used for duck typing Collection objects with Spec and Match
        in source and sink initiation. Checks IDs match sessions in study.
        """
        if self.frequency == 'per_subject':
            tree_subject_ids = list(study.tree.subject_ids)
            subject_ids = list(self._collection.keys())
            if tree_subject_ids != subject_ids:
                raise ArcanaUsageError(
                    "Subject IDs in collection provided to '{}' ('{}') "
                    "do not match Study tree ('{}')".format(
                        self.name, "', '".join(subject_ids),
                        "', '".join(tree_subject_ids)))
        elif self.frequency == 'per_visit':
            tree_visit_ids = list(study.tree.visit_ids)
            visit_ids = list(self._collection.keys())
            if tree_visit_ids != visit_ids:
                raise ArcanaUsageError(
                    "Subject IDs in collection provided to '{}' ('{}') "
                    "do not match Study tree ('{}')".format(
                        self.name, "', '".join(visit_ids),
                        "', '".join(tree_visit_ids)))
        elif self.frequency == 'per_session':
            for subject in study.tree.subjects:
                if subject.id not in self._collection:
                    raise ArcanaUsageError(
                        "Study subject ID '{}' was not found in colleciton "
                        "provided to '{}' (found '{}')".format(
                            subject.id, self.name,
                            "', '".join(self._collection.keys())))
                for session in subject.sessions:
                    if session.visit_id not in self._collection[subject.id]:
                        raise ArcanaUsageError(
                            "Study visit ID '{}' for subject '{}' was not "
                            "found in colleciton provided to '{}' (found '{}')"
                            .format(subject.id, self.name,
                                    "', '".join(
                                        self._collection[subject.id].keys())))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name


class FilesetCollection(BaseCollection, BaseFileset):
    """
    A collection of filesets across a study (typically within a repository)

    Parameters
    ----------
    name : str
        Name of the collection
    collection : List[Fileset]
        An iterable of equivalent filesets
    frequency : str
        The frequency of the collection
    format : FileFormat | None
        The file format of the collection (will be determined from filesets
        if not provided).
    """

    CollectedClass = Fileset

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
                    "frequency '{}' for '{}' FilesetCollection"
                    .format(implicit_frequency, frequency, name))
            implicit_format = self._common_attr(collection, 'format')
            if format is None:
                format = implicit_format  # @ReservedAssignment
            elif format != implicit_format:
                raise ArcanaUsageError(
                    "Implicit format '{}' does not match explicit "
                    "format '{}' for '{}' FilesetCollection"
                    .format(implicit_format, format, name))
        if frequency is None:
            raise ArcanaUsageError(
                "Need to provide explicit frequency for empty "
                "FilesetCollection")
        if format is None:
            raise ArcanaUsageError(
                "Need to provide explicit format for empty "
                "FilesetCollection")
        BaseFileset.__init__(self, name, format, frequency=frequency)
        BaseCollection.__init__(self, collection, frequency)

    def path(self, subject_id=None, visit_id=None):
        return self.item(
            subject_id=subject_id, visit_id=visit_id).path


class FieldCollection(BaseCollection, BaseField):
    """
    A collection of equivalent filesets (either within a repository)

    Parameters
    ----------
    name : str
        Name of the collection
    collection : List[Fileset]
        An iterable of equivalent filesets
    """

    CollectedClass = Field

    def __init__(self, name, collection, frequency=None, dtype=None,
                 array=None):
        collection = list(collection)
        if collection:
            implicit_frequency = self._common_attr(collection,
                                                   'frequency')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for '{}' FilesetCollection"
                    .format(implicit_frequency, frequency, name))
            implicit_dtype = self._common_attr(collection, 'dtype')
            if dtype is None:
                dtype = implicit_dtype  # @ReservedAssignment
            elif dtype != implicit_dtype:
                raise ArcanaUsageError(
                    "Implicit dtype '{}' does not match explicit "
                    "dtype '{}' for '{}' FilesetCollection"
                    .format(implicit_dtype, dtype, name))
            implicit_array = self._common_attr(collection, 'array')
            if array is None:
                array = implicit_array
            elif array != implicit_array:
                raise ArcanaUsageError(
                    "Implicit array '{}' does not match explicit "
                    "array '{}' for '{}' FilesetCollection"
                    .format(implicit_array, array, name))
        if frequency is None:
            raise ArcanaUsageError(
                "Need to provide explicit frequency for empty "
                "FilesetCollection")
        if dtype is None:
            raise ArcanaUsageError(
                "Need to provide explicit dtype for empty "
                "FilesetCollection")
        BaseField.__init__(self, name, dtype=dtype, frequency=frequency,
                           array=array)
        BaseCollection.__init__(self, collection, frequency)
