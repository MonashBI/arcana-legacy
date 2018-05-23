import re
from itertools import chain
from copy import copy
from collections import defaultdict
from arcana.exception import (
    ArcanaError, ArcanaUsageError,
    ArcanaDatasetMatchError)
from .base import BaseDataset, BaseField


class BaseMatch(object):
    """
    Base class for Dataset and Field Match classes
    """

    def __init__(self, pattern, derived, order, study, is_regex):
        self._derived = derived
        self._order = order
        self._pattern = pattern
        self._study = study
        self._is_regex = is_regex
        self._matches = None

    def __eq__(self, other):
        return (self.derived == other.derived and
                self.pattern == other.pattern and
                self.order == other.order and
                self._study == other._study)

    @property
    def pattern(self):
        return self._pattern

    @property
    def derived(self):
        return self._derived

    @property
    def study(self):
        return self._study

    @property
    def matches(self):
        if self.frequency == 'per_session':
            matches = chain(*(d.itervalues()
                              for d in self._matches.itervalues()))
        elif self.frequency == 'per_subject':
            matches = self._matches.itervalues()
        elif self.frequency == 'per_visit':
            matches = self._matches.itervalues()
        elif self.frequency == 'per_project':
            self._matches = iter([self._matches])
        else:
            assert False
        return matches

    @property
    def is_regex(self):
        return self._is_regex

    @property
    def order(self):
        return self._order

    def bind(self, study):
        if self._study is not None:
            bound = self
        else:
            bound = copy(self)
            bound._study = study
            bound._match_tree(study.tree)
        return bound

    @property
    def prefixed_name(self):
        return self.name

    def basename(self, **kwargs):
        if not self.is_regex:
            basename = self.pattern
        else:
            basename = self.match(**kwargs).name
        return basename

    def _match_tree(self, tree, **kwargs):
        # Run the match against the tree
        if self.frequency == 'per_session':
            self._matches = defaultdict(dict)
            for subject in tree.subjects:
                for sess in subject.sessions:
                    if self.derived and sess.derived is not None:
                        node = sess.derived
                    else:
                        node = sess
                    self._matches[sess.subject_id][
                        sess.visit_id] = self._match_node(node,
                                                          **kwargs)
        elif self.frequency == 'per_subject':
            self._matches = {}
            for subject in tree.subjects:
                self._matches[subject.id] = self._match_node(subject,
                                                             **kwargs)
        elif self.frequency == 'per_visit':
            self._matches = {}
            for visit in tree.visits:
                self._matches[visit.id] = self._match_node(visit,
                                                           **kwargs)
        elif self.frequency == 'per_project':
            self._matches = self._match_node(tree, **kwargs)
        else:
            assert False, "Unrecognised frequency '{}'".format(
                self.frequency)

    def _match_node(self, node, **kwargs):
        # Get names matching pattern
        matches = self._filtered_matches(node, **kwargs)
        # Select the dataset from the matches
        if self.order is not None:
            try:
                match = matches[self.order]
            except IndexError:
                raise ArcanaDatasetMatchError(
                    "Did not find {} datasets names matching pattern {}"
                    " (found {}) in {}".format(self.order, self.pattern,
                                               len(matches), node))
        elif len(matches) == 1:
            match = matches[0]
        elif matches:
            raise ArcanaDatasetMatchError(
                "Found multiple matches for {} pattern in {} ({})"
                .format(self.pattern, node,
                        ', '.join(str(m) for m in matches)))
        else:
            raise ArcanaDatasetMatchError(
                "Did not find any matches for {} pattern in {} "
                "(found {})"
                .format(self.pattern, node,
                        ', '.join(str(d) for d in node.datasets)))
        return match

    def match(self, subject_id=None, visit_id=None):
        if self._matches is None:
            raise ArcanaError(
                "{} has not been bound to study".format(self))
        if self.frequency == 'per_session':
            if subject_id is None or visit_id is None:
                raise ArcanaError(
                    "The 'subject_id' and 'visit_id' must be provided "
                    "to get the match from {}".format(self))
            dataset = self._matches[subject_id][visit_id]
        elif self.frequency == 'per_subject':
            if subject_id is None:
                raise ArcanaError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            dataset = self._matches[subject_id]
        elif self.frequency == 'per_visit':
            if visit_id is None:
                raise ArcanaError(
                    "The 'visit_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            dataset = self._matches[visit_id]
        elif self.frequency == 'per_project':
            dataset = self._matches
        return dataset

    def initkwargs(self):
        dct = {}
        dct['derived'] = self.derived
        dct['study'] = self._study
        dct['pattern'] = self.pattern
        dct['order'] = self.order
        return dct


class DatasetMatch(BaseDataset, BaseMatch):
    """
    A pattern that describes a single dataset (typically acquired
    rather than generated but not necessarily) within each session.

    Parameters
    ----------
    name : str
        The name of the dataset, typically left None and set in Study
    pattern : str
        A regex pattern to match the dataset names with. Must match
        one and only one dataset per <frequency>. If None, the name
        is used instead.
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    derived : bool
        Whether the scan was generated or acquired. Depending on the archive
        used to store the dataset this is used to determine the location of the
        dataset.
    id : int | None
        To be used to distinguish multiple datasets that match the
        pattern in the same session. The ID of the dataset within the
        session.
    order : int | None
        To be used to distinguish multiple datasets that match the
        pattern in the same session. The order of the dataset within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    dicom_tags : dct(str | str)
        To be used to distinguish multiple datasets that match the
        pattern in the same session. The provided DICOM values dicom
        header values must match exactly.
    """

    is_spec = False

    def __init__(self, name, format, pattern=None, # @ReservedAssignment @IgnorePep8
                 frequency='per_session', derived=False, id=None,  # @ReservedAssignment @IgnorePep8
                 order=None, dicom_tags=None, is_regex=False,
                 study=None):
        if pattern is None and id is None:
            raise ArcanaUsageError(
                "Either 'pattern' or 'id' need to be provided to "
                "DatasetMatch constructor")
        BaseDataset.__init__(self, name, format, frequency)
        BaseMatch.__init__(self, pattern, derived, order, study,
                           is_regex)
        if dicom_tags is not None and format.name != 'dicom':
            raise ArcanaUsageError(
                "Cannot use 'dicom_tags' kwarg with non-DICOM "
                "format ({})".format(format))
        self._dicom_tags = dicom_tags
        if order is not None and id is not None:
            raise ArcanaUsageError(
                "Cannot provide both 'order' and 'id' to a dataset"
                "match")
        self._id = str(id) if id is not None else id

    def __eq__(self, other):
        return (BaseDataset.__eq__(self, other) and
                BaseMatch.__eq__(self, other) and
                self.dicom_tags == other.dicom_tags and
                self.id == other.id)

    def initkwargs(self):
        dct = BaseDataset.initkwargs(self)
        dct.update(BaseMatch.initkwargs(self))
        dct['dicom_tags'] = self.dicom_tags
        dct['id'] = self.id
        return dct

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency={}, derived={},"
                " pattern={}, order={}, id={}, dicom_tags={}, "
                "is_regex={}, study={})"
                .format(self.__class__.__name__, self.name, self.format,
                        self.frequency, self.derived, self._pattern,
                        self.order, self.id, self.dicom_tags,
                        self.is_regex, self._study))

    @property
    def id(self):
        return self._id

    @property
    def dicom_tags(self):
        return self._dicom_tags

    def _match_tree(self, tree, **kwargs):
        with self.study.archive.login() as archive_login:
            super(DatasetMatch, self)._match_tree(
                tree, archive_login=archive_login, **kwargs)

    def _filtered_matches(self, node, archive_login=None):
        if self.pattern is not None:
            if self.is_regex:
                pattern_re = re.compile(self.pattern)
                matches = [d for d in node.datasets
                           if pattern_re.match(d.name)]
            else:
                matches = [d for d in node.datasets
                           if d.name == self.pattern]
        else:
            matches = list(node.datasets)
        if not matches:
            raise ArcanaDatasetMatchError(
                "No dataset names in {} match '{}' pattern:{}"
                .format(node, self.pattern,
                        '\n'.join(d.name for d in node.datasets)))
        if self.id is not None:
            filtered = [d for d in matches if d.id == self.id]
            if not filtered:
                raise ArcanaDatasetMatchError(
                    "Did not find datasets names matching pattern {}"
                    "with an id of {} (found {}) in {}".format(
                        self.pattern, self.id,
                        ', '.join(str(m) for m in matches), node))
            matches = filtered
        # Filter matches by dicom tags
        if self.dicom_tags is not None:
            filtered = []
            for dataset in matches:
                values = dataset.dicom_values(
                    self.dicom_tags.keys(), archive_login=archive_login)
                if self.dicom_tags == values:
                    filtered.append(dataset)
            if not filtered:
                raise ArcanaDatasetMatchError(
                    "Did not find datasets names matching pattern {}"
                    "that matched DICOM tags {} (found {}) in {}"
                    .format(self.pattern, self.dicom_tags,
                            ', '.join(str(m) for m in matches), node))
            matches = filtered
        return matches


class FieldMatch(BaseField, BaseMatch):
    """
    A pattern that matches a single field (typically acquired rather than
    generated but not necessarily) in each session.

    Parameters
    ----------
    name : str
        The name of the dataset
    pattern : str
        A regex pattern to match the field names with. Must match
        one and only one dataset per <frequency>. If None, the name
        is used instead.
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    derived : bool
        Whether or not the value belongs in the derived session or not
    order : int | None
        To be used to distinguish multiple datasets that match the
        pattern in the same session. The order of the dataset within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    """

    is_spec = False

    def __init__(self, name, dtype, pattern, frequency='per_session',
                 derived=False, order=None, is_regex=False, study=None):
        BaseField.__init__(self, name, dtype, frequency)
        BaseMatch.__init__(self, pattern, derived, order, study,
                           is_regex)
        super(FieldMatch, self).__init__(name, dtype, frequency)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseMatch.__eq__(self, other))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseMatch.initkwargs(self))
        return dct

    def _filtered_matches(self, node):
        if self.is_regex:
            pattern_re = re.compile(self.pattern)
            matches = [f for f in node.fields
                       if pattern_re.match(f.name)]
        else:
            matches = [f for f in node.fields
                       if f.name == self.pattern]
        if not matches:
            raise ArcanaDatasetMatchError(
                "No field names in {} match '{}' pattern"
                .format(node, self.pattern))
        return matches

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, derived={},"
                " pattern={}, order={}, is_regex={}, study={})"
                .format(self.__class__.__name__, self.name, self.dtype,
                        self.frequency, self.derived,
                        self._pattern, self.order, self.is_regex,
                        self._study))
