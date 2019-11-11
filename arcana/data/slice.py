from copy import copy
from arcana.exceptions import (
    ArcanaError, ArcanaUsageError, ArcanaIndexError)
from .base import BaseFileset, BaseField
from .item import Fileset, Field
from collections import OrderedDict
from operator import itemgetter
from itertools import chain


DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class BaseSliceMixin(object):
    """
    Base class for slce of filesets and field items
    """

    # For duck-typing with *Spec and *Input objects
    is_spec = False
    skip_missing = False
    drop_if_missing = False
    derivable = False

    def __init__(self, slce, frequency):
        self._frequency = frequency
        if frequency == 'per_dataset':
            # If wrapped in an iterable
            if not isinstance(slce, self.SlicedClass):
                if len(slce) > 1:
                    raise ArcanaUsageError(
                        "More than one {} passed to {}"
                        .format(self.CONTAINED_CLASS.__name__,
                                type(self).__name__))
                slce = list(slce)
            self._slice = slce
        elif frequency == 'per_session':
            self._slice = OrderedDict()
            for subj_id in sorted(set(c.subject_id for c in slce)):
                self._slice[subj_id] = OrderedDict(
                    sorted(((c.visit_id, c) for c in slce
                            if c.subject_id == subj_id),
                           key=itemgetter(0)))
        elif frequency == 'per_subject':
            self._slice = OrderedDict(
                sorted(((c.subject_id, c) for c in slce),
                       key=itemgetter(0)))
        elif frequency == 'per_visit':
            self._slice = OrderedDict(
                sorted(((c.visit_id, c) for c in slce),
                       key=itemgetter(0)))
        else:
            assert False
        for datum in self:
            if not isinstance(datum, self.SlicedClass):
                raise ArcanaUsageError(
                    "Invalid class {} in {}".format(datum, self))

    def __iter__(self):
        if self._frequency == 'per_dataset':
            return iter(self._slice)
        elif self._frequency == 'per_session':
            return chain(*(c.values()
                           for c in self._slice.values()))
        else:
            return iter(self._slice.values())

    def __len__(self):
        if self.frequency == 'per_session':
            ln = sum(len(d) for d in self._slice.values())
        else:
            ln = len(self._slice)
        return ln

    def _common_attr(self, slce, attr_name, ignore_none=True):
        attr_set = set(getattr(c, attr_name) for c in slce)
        if ignore_none:
            attr_set -= set([None])
        if len(attr_set) > 1:
            raise ArcanaUsageError(
                "Heterogeneous attributes for '{}' within {}".format(
                    attr_name, self))
        try:
            return next(iter(attr_set))
        except StopIteration:
            return None

    def item(self, subject_id=None, visit_id=None):
        """
        Returns a particular fileset|field in the slce corresponding to
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
                subj_dct = self._slice[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' slce ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._slice.keys())))
            try:
                fileset = subj_dct[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in subject {} of '{}' "
                    "slce ({})"
                    .format(visit_id, subject_id, self.name,
                            ', '.join(subj_dct.keys())))
        elif self.frequency == 'per_subject':
            if subject_id is None:
                raise ArcanaError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                fileset = self._slice[subject_id]
            except KeyError:
                raise ArcanaIndexError(
                    subject_id,
                    "{} not a subject ID in '{}' slce ({})"
                    .format(subject_id, self.name,
                            ', '.join(self._slice.keys())))
        elif self.frequency == 'per_visit':
            if visit_id is None:
                raise ArcanaError(
                    "The 'visit_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            try:
                fileset = self._slice[visit_id]
            except KeyError:
                raise ArcanaIndexError(
                    visit_id,
                    "{} not a visit ID in '{}' slce ({})"
                    .format(visit_id, self.name,
                            ', '.join(self._slice.keys())))
        elif self.frequency == 'per_dataset':
            try:
                fileset = self._slice[0]
            except IndexError:
                raise ArcanaIndexError(
                    0, ("'{}' Slice is empty so doesn't have a " +
                        "per_dataset node").format(self.name))
        return fileset

    @property
    def slce(self):
        "Used for duck typing Slice objects with Spec and Match "
        "in source and sink initiation"
        return self

    def bind(self, analysis, **kwargs):
        """
        Used for duck typing Slice objects with Spec and Match
        in source and sink initiation. Checks IDs match sessions in analysis.
        """
        if self.frequency == 'per_subject':
            tree_subject_ids = list(analysis.dataset.tree.subject_ids)
            subject_ids = list(self._slice.keys())
            if tree_subject_ids != subject_ids:
                raise ArcanaUsageError(
                    "Subject IDs in slce provided to '{}' ('{}') "
                    "do not match Analysis tree ('{}')".format(
                        self.name, "', '".join(subject_ids),
                        "', '".join(tree_subject_ids)))
        elif self.frequency == 'per_visit':
            tree_visit_ids = list(analysis.dataset.tree.visit_ids)
            visit_ids = list(self._slice.keys())
            if tree_visit_ids != visit_ids:
                raise ArcanaUsageError(
                    "Subject IDs in slce provided to '{}' ('{}') "
                    "do not match Analysis tree ('{}')".format(
                        self.name, "', '".join(visit_ids),
                        "', '".join(tree_visit_ids)))
        elif self.frequency == 'per_session':
            for subject in analysis.dataset.tree.subjects:
                if subject.id not in self._slice:
                    raise ArcanaUsageError(
                        "Analysis subject ID '{}' was not found in colleciton "
                        "provided to '{}' (found '{}')".format(
                            subject.id, self.name,
                            "', '".join(self._slice.keys())))
                for session in subject.sessions:
                    if session.visit_id not in self._slice[subject.id]:
                        raise ArcanaUsageError(
                            ("Analysis visit ID '{}' for subject '{}' was not "
                             + "found in colleciton provided to '{}' "
                             + "(found '{}')").format(
                                 subject.id, self.name,
                                 "', '".join(
                                     self._slice[subject.id].keys())))

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name


class FilesetSlice(BaseSliceMixin, BaseFileset):
    """
    A slce of filesets across a analysis (typically within a repository)

    Parameters
    ----------
    name : str
        Name of the slce
    slce : List[Fileset]
        An iterable of equivalent filesets
    frequency : str
        The frequency of the slce
    format : FileFormat | None
        The file format of the slce (will be determined from filesets
        if not provided).
    """

    SlicedClass = Fileset

    def __init__(self, name, slce, format=None, frequency=None,
                 candidate_formats=None):
        if format is None and candidate_formats is None:
            raise ArcanaUsageError(
                "Either 'format' or candidate_formats needs to be supplied "
                "during the initialisation of a FilesetSlice ('{}')"
                .format(name))
        slce = list(slce)
        if not slce:
            if format is None:
                format = candidate_formats[0]
            if frequency is None:
                raise ArcanaUsageError(
                    "Need to provide explicit frequency for empty "
                    "FilesetSlice")
        else:
            implicit_frequency = self._common_attr(slce,
                                                   'frequency')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for '{}' FilesetSlice"
                    .format(implicit_frequency, frequency, name))
            formatted_slice = []
            for fileset in slce:
                fileset = copy(fileset)
                if fileset.exists and fileset.format is None:
                    fileset.format = (fileset.detect_format(candidate_formats)
                                      if format is None else format)
                formatted_slice.append(fileset)
            slce = formatted_slice
            format = self._common_attr(slce, 'format')
        BaseFileset.__init__(self, name, format, frequency=frequency)
        BaseSliceMixin.__init__(self, slce, frequency)

    def path(self, subject_id=None, visit_id=None):
        return self.item(
            subject_id=subject_id, visit_id=visit_id).path


class FieldSlice(BaseSliceMixin, BaseField):
    """
    A slce of equivalent filesets (either within a repository)

    Parameters
    ----------
    name : str
        Name of the slce
    slce : List[Fileset]
        An iterable of equivalent filesets
    """

    SlicedClass = Field

    def __init__(self, name, slce, frequency=None, dtype=None,
                 array=None):
        slce = list(slce)
        if slce:
            implicit_frequency = self._common_attr(slce,
                                                   'frequency')
            if frequency is None:
                frequency = implicit_frequency
            elif frequency != implicit_frequency:
                raise ArcanaUsageError(
                    "Implicit frequency '{}' does not match explicit "
                    "frequency '{}' for '{}' FieldSlice"
                    .format(implicit_frequency, frequency, name))
            implicit_dtype = self._common_attr(slce, 'dtype')
            if dtype is None:
                dtype = implicit_dtype
            elif dtype != implicit_dtype:
                raise ArcanaUsageError(
                    "Implicit dtype '{}' does not match explicit "
                    "dtype '{}' for '{}' FieldSlice"
                    .format(implicit_dtype, dtype, name))
            implicit_array = self._common_attr(slce, 'array')
            if array is None:
                array = implicit_array
            elif array != implicit_array:
                raise ArcanaUsageError(
                    "Implicit array '{}' does not match explicit "
                    "array '{}' for '{}' FieldSlice"
                    .format(implicit_array, array, name))
        if frequency is None:
            raise ArcanaUsageError(
                "Need to provide explicit frequency for empty "
                "FieldSlice")
        if dtype is None:
            raise ArcanaUsageError(
                "Need to provide explicit dtype for empty "
                "FieldSlice")
        BaseField.__init__(self, name, dtype=dtype, frequency=frequency,
                           array=array)
        BaseSliceMixin.__init__(self, slce, frequency)

    def value(self, subject_id=None, visit_id=None):
        return self.item(subject_id=subject_id, visit_id=visit_id).value
