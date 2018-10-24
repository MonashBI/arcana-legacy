from builtins import object
import re
from arcana.utils import ExitStack
from copy import copy
from itertools import chain
from arcana.exception import (
    ArcanaUsageError, ArcanaFilesetSelectorError)
from .base import BaseFileset, BaseField
from .collection import FilesetCollection, FieldCollection


class BaseMatch(object):
    """
    Base class for Fileset and Field Match classes
    """

    def __init__(self, pattern, is_regex, order, from_study,
                 repository=None, study_=None, collection_=None):
        self._pattern = pattern
        self._is_regex = is_regex
        self._order = order
        self._from_study = from_study
        self._repository = repository
        # study_ and collection_ are not intended to be provided to __init__
        # except when recreating when using initkwargs
        self._study = study_
        self._collection = collection_

    def __eq__(self, other):
        return (self.from_study == other.from_study and
                self.pattern == other.pattern and
                self.is_regex == other.is_regex and
                self.order == other.order and
                self.repository == other.repository)

    def __hash__(self):
        return (hash(self.from_study) ^
                hash(self.pattern) ^
                hash(self.is_regex) ^
                hash(self.order) ^
                hash(self.repository))

    @property
    def pattern(self):
        return self._pattern

    @property
    def derived(self):
        return self._from_study is not None

    @property
    def from_study(self):
        return self._from_study

    @property
    def repository(self):
        return self._repository

    @property
    def collection(self):
        if self._collection is None:
            raise ArcanaUsageError(
                "{} has not been bound to a study".format(self))
        return self._collection

    @property
    def is_regex(self):
        return self._is_regex

    @property
    def order(self):
        return self._order

    def bind(self, study, **kwargs):
        if self._study == study:
            bound = self
        else:
            # Use the default study repository if not explicitly
            # provided to match
            if self._repository is None:
                tree = study.tree
            else:
                tree = self._repository.cached_tree(
                    subject_ids=study.subject_ids,
                    visit_ids=study.visit_ids)
            # Match against tree
            bound = copy(self)
            bound._study = study
            bound._collection = self.match(tree, **kwargs)
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

    def match(self, tree, **kwargs):
        # Run the match against the tree
        if self.frequency == 'per_session':
            nodes = chain(*(s.sessions for s in tree.subjects))
        elif self.frequency == 'per_subject':
            nodes = tree.subjects
        elif self.frequency == 'per_visit':
            nodes = tree.visits
        elif self.frequency == 'per_study':
            nodes = [tree]
        else:
            assert False, "Unrecognised frequency '{}'".format(
                self.frequency)
        return self.CollectionClass(
            self.name, (self._match_node(n, **kwargs)
                        for n in nodes),
            frequency=self.frequency,
            **self._specific_collection_kwargs)

    def _match_node(self, node, **kwargs):
        # Get names matching pattern
        matches = self._filtered_matches(node, **kwargs)
        # Filter matches by study name
        matches = [d for d in matches
                   if d.from_study == self.from_study]
        # Select the fileset from the matches
        if self.order is not None:
            try:
                match = matches[self.order]
            except IndexError:
                raise ArcanaFilesetSelectorError(
                    "Did not find {} filesets names matching pattern {}"
                    " (found {}) in {}".format(self.order, self.pattern,
                                               len(matches), node))
        elif len(matches) == 1:
            match = matches[0]
        elif matches:
            raise ArcanaFilesetSelectorError(
                "Found multiple matches for {} pattern in {} ({})"
                .format(self.pattern, node,
                        ', '.join(str(m) for m in matches)))
        else:
            raise ArcanaFilesetSelectorError(
                "Did not find any matches for {} pattern in {} "
                "(found {})"
                .format(self.pattern, node,
                        ', '.join(str(d) for d in node.filesets)))
        return match

    def initkwargs(self):
        dct = {}
        dct['from_study'] = self.from_study
        dct['pattern'] = self.pattern
        dct['order'] = self.order
        dct['is_regex'] = self.is_regex
        dct['study_'] = self._study
        dct['collection_'] = self._collection
        return dct


class FilesetSelector(BaseMatch, BaseFileset):
    """
    A pattern that describes a single fileset (typically acquired
    rather than generated but not necessarily) within each session.

    Parameters
    ----------
    name : str
        The name of the fileset, typically left None and set in Study
    format : FileFormat
        The file format used to store the fileset. Can be one of the
        recognised formats
    pattern : str
        A regex pattern to match the fileset names with. Must match
        one and only one fileset per <frequency>. If None, the name
        is used instead.
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    id : int | None
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The ID of the fileset within the
        session.
    order : int | None
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The order of the fileset within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    dicom_tags : dct(str | str)
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The provided DICOM values dicom
        header values must match exactly.
    from_study : str
        The name of the study that generated the derived fileset to match.
        Is used to determine the location of the filesets in the
        repository as the derived filesets and fields are grouped by
        the name of the study that generated them.
    repository : BaseRepository | None
        The repository to draw the matches from, if not the main repository
        that is used to store the products of the current study.
    """

    is_spec = False
    CollectionClass = FilesetCollection

    def __init__(self, name, format, pattern=None, # @ReservedAssignment @IgnorePep8
                 frequency='per_session', id=None,  # @ReservedAssignment @IgnorePep8
                 order=None, dicom_tags=None, is_regex=False,
                 from_study=None, repository=None, study_=None,
                 collection_=None):
        if pattern is None and id is None:
            raise ArcanaUsageError(
                "Either 'pattern' or 'id' need to be provided to "
                "FilesetSelector constructor")
        BaseFileset.__init__(self, name, format, frequency)
        BaseMatch.__init__(self, pattern, is_regex, order,
                           from_study, repository, study_, collection_)
        if dicom_tags is not None and format.name != 'dicom':
            raise ArcanaUsageError(
                "Cannot use 'dicom_tags' kwarg with non-DICOM "
                "format ({})".format(format))
        self._dicom_tags = dicom_tags
        if order is not None and id is not None:
            raise ArcanaUsageError(
                "Cannot provide both 'order' and 'id' to a fileset"
                "match")
        self._id = str(id) if id is not None else id

    def __eq__(self, other):
        return (BaseFileset.__eq__(self, other) and
                BaseMatch.__eq__(self, other) and
                self.dicom_tags == other.dicom_tags and
                self.id == other.id)

    def __hash__(self):
        return (BaseFileset.__hash__(self) ^
                BaseMatch.__hash__(self) ^
                hash(self.dicom_tags) ^
                hash(self.id))

    def initkwargs(self):
        dct = BaseFileset.initkwargs(self)
        dct.update(BaseMatch.initkwargs(self))
        dct['dicom_tags'] = self.dicom_tags
        dct['id'] = self.id
        return dct

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency={}, pattern={}, "
                "is_regex={}, order={}, id={}, dicom_tags={}, "
                "from_study={})"
                .format(self.__class__.__name__, self.name, self.format,
                        self.frequency, self._pattern, self.is_regex,
                        self.order, self.id, self.dicom_tags,
                        self._from_study))

    @property
    def id(self):
        return self._id

    def bind(self, study, **kwargs):
        with ExitStack() as stack:
            # If dicom tags are used to match against then a connection
            # to the repository may be required to query them.
            if self.dicom_tags is not None and self._study != study:
                stack.enter_context(study.repository)
            return super(FilesetSelector, self).bind(study, **kwargs)

    @property
    def dicom_tags(self):
        return self._dicom_tags

    def _filtered_matches(self, node):
        if self.pattern is not None:
            if self.is_regex:
                pattern_re = re.compile(self.pattern)
                matches = [d for d in node.filesets
                           if pattern_re.match(d.name)]
            else:
                matches = [d for d in node.filesets
                           if d.name == self.pattern]
        else:
            matches = list(node.filesets)
        if not matches:
            raise ArcanaFilesetSelectorError(
                "No fileset names in {}:{} match '{}' pattern, found: {}"
                .format(node.subject_id, node.visit_id, self.pattern,
                        ', '.join(d.name for d in node.filesets)))
        if self.id is not None:
            filtered = [d for d in matches if d.id == self.id]
            if not filtered:
                raise ArcanaFilesetSelectorError(
                    "Did not find filesets names matching pattern {} "
                    "with an id of {} (found {}) in {}".format(
                        self.pattern, self.id,
                        ', '.join(str(m) for m in matches), node))
            matches = filtered
        # Filter matches by dicom tags
        if self.dicom_tags is not None:
            filtered = []
            for fileset in matches:
                values = fileset.dicom_values(
                    list(self.dicom_tags.keys()))
                if self.dicom_tags == values:
                    filtered.append(fileset)
            if not filtered:
                raise ArcanaFilesetSelectorError(
                    "Did not find filesets names matching pattern {}"
                    "that matched DICOM tags {} (found {}) in {}"
                    .format(self.pattern, self.dicom_tags,
                            ', '.join(str(m) for m in matches), node))
            matches = filtered
        return matches

    @property
    def _specific_collection_kwargs(self):
        return {'format': self.format}


class FieldSelector(BaseMatch, BaseField):
    """
    A pattern that matches a single field (typically acquired rather than
    generated but not necessarily) in each session.

    Parameters
    ----------
    name : str
        The name of the fileset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pattern : str
        A regex pattern to match the field names with. Must match
        one and only one fileset per <frequency>. If None, the name
        is used instead.
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    order : int | None
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The order of the fileset within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    from_study : str
        The name of the study that generated the derived field to match.
        Is used to determine the location of the fields in the
        repository as the derived filesets and fields are grouped by
        the name of the study that generated them.
    repository : BaseRepository | None
        The repository to draw the matches from, if not the main repository
        that is used to store the products of the current study.
    """

    is_spec = False
    CollectionClass = FieldCollection

    def __init__(self, name, dtype, pattern, frequency='per_session',
                 order=None, is_regex=False, from_study=None,
                 repository=None, study_=None, collection_=None):
        BaseField.__init__(self, name, dtype, frequency)
        BaseMatch.__init__(self, pattern, is_regex, order,
                           from_study, repository,
                           study_, collection_)
        super(FieldSelector, self).__init__(name, dtype, frequency)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseMatch.__eq__(self, other))

    def __hash__(self):
        return (BaseField.__hash__(self) ^ BaseMatch.__hash__(self))

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
        if self.from_study is not None:
            matches = [f for f in matches
                       if f.from_study == self.from_study]
        if not matches:
            raise ArcanaFilesetSelectorError(
                "No field names in {} match '{}' pattern"
                .format(node, self.pattern))
        return matches

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, pattern={}, "
                "is_regex={}, order={}, from_study={})"
                .format(self.__class__.__name__, self.name, self.dtype,
                        self.frequency, self._pattern, self.is_regex,
                        self.order, self._from_study))

    @property
    def _specific_collection_kwargs(self):
        return {'dtype': self.dtype}
