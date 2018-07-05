from builtins import str
from builtins import object
import re
from copy import copy
from arcana.exception import (
    ArcanaUsageError, ArcanaDatasetMatchError)
from .base import BaseDataset, BaseField
from .collection import DatasetCollection, FieldCollection


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
        self._collection = None

    def __eq__(self, other):
        return (self.derived == other.derived and
                self.pattern == other.pattern and
                self.order == other.order)

    def __hash__(self):
        return (hash(self.derived) ^
                hash(self.pattern) ^
                hash(self.order))

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
    def is_regex(self):
        return self._is_regex

    @property
    def collection(self):
        if self._collection is None:
            raise ArcanaUsageError(
                "{} needs to be bound to a study before accessing "
                "the corresponding collection".format(self))
        return self._collection

    @property
    def order(self):
        return self._order

    def bind(self, study):
        if self._study is not None:
            bound = self
        else:
            bound = copy(self)
            bound._study = study
            bound._bind_tree(study.tree)
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

    def _bind_tree(self, tree, **kwargs):
        # Run the match against the tree
        if self.frequency == 'per_session':
            nodes = []
            for subject in tree.subjects:
                for sess in subject.sessions:
                    if self.derived and sess.derived is not None:
                        nodes.append(sess.derived)
                    else:
                        nodes.append(sess)
        elif self.frequency == 'per_subject':
            nodes = tree.subjects
        elif self.frequency == 'per_visit':
            nodes = tree.visits
        elif self.frequency == 'per_project':
            nodes = [tree]
        else:
            assert False, "Unrecognised frequency '{}'".format(
                self.frequency)
        self._collection = self.CollectionClass(
            self.name, (self._bind_node(n, **kwargs) for n in nodes))

    def _bind_node(self, node, **kwargs):
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
        return self._matches.particular(subject_id=subject_id,
                                        visit_id=visit_id)

    def initkwargs(self):
        dct = {}
        dct['derived'] = self.derived
        dct['study'] = self._study
        dct['pattern'] = self.pattern
        dct['order'] = self.order
        return dct


class DatasetMatch(BaseMatch, BaseDataset):
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
        Whether the scan was generated or acquired. Depending on the repository
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
    CollectionClass = DatasetCollection

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

    def __hash__(self):
        return (BaseDataset.__hash__(self) ^
                BaseMatch.__hash__(self) ^
                hash(self.dicom_tags) ^
                hash(self.id))

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

    def _bind_tree(self, tree, **kwargs):
        with self.study.repository:
            super(DatasetMatch, self)._bind_tree(tree, **kwargs)

    def _filtered_matches(self, node, repository_login=None):
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
                "No dataset names in {}:{} match '{}' pattern, found: {}"
                .format(node.subject_id, node.visit_id, self.pattern,
                        ', '.join(d.name for d in node.datasets)))
        if self.id is not None:
            filtered = [d for d in matches if d.id == self.id]
            if not filtered:
                raise ArcanaDatasetMatchError(
                    "Did not find datasets names matching pattern {} "
                    "with an id of {} (found {}) in {}".format(
                        self.pattern, self.id,
                        ', '.join(str(m) for m in matches), node))
            matches = filtered
        # Filter matches by dicom tags
        if self.dicom_tags is not None:
            filtered = []
            for dataset in matches:
                values = dataset.dicom_values(
                    list(self.dicom_tags.keys()),
                    repository_login=repository_login)
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


class FieldMatch(BaseMatch, BaseField):
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
    CollectionClass = FieldCollection

    def __init__(self, name, dtype, pattern, frequency='per_session',
                 derived=False, order=None, is_regex=False, study=None):
        BaseField.__init__(self, name, dtype, frequency)
        BaseMatch.__init__(self, pattern, derived, order, study,
                           is_regex)
        super(FieldMatch, self).__init__(name, dtype, frequency)

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


class BidsDatasetMatch(DatasetMatch):
    """
    A match object for matching datasets from their 'bids_attr'
    attribute

    Parameters
    ----------
    name : str
        Name of the dataset
    type : str
        Type of the dataset
    modality : str
        Modality of the datasets
    format : FileFormat
        The file format of the dataset to match
    run : int
        Run number of the dataset
    """

    def __init__(self, name, type, modality, format, run=None):  # @ReservedAssignment @IgnorePep8
        DatasetMatch.__init__(
            self, name, format, pattern=None, frequency='per_session',   # @ReservedAssignment @IgnorePep8
            derived=False, id=None, order=run, dicom_tags=None,
            is_regex=False, study=None)
        self._type = type
        self._modality = modality
        self._run = run

    @property
    def type(self):
        return self._type

    @property
    def modality(self):
        return self._modality

    @property
    def run(self):
        return self.order

    def _filtered_matches(self, node):
        matches = [
            d for d in node.datasets
            if (d.bids_attr.entities['type'] == self.type and
                d.bids_attr.entities['modality'] == self.modality)]
        if not matches:
            raise ArcanaDatasetMatchError(
                "No BIDS datasets for subject={}, visit={} match "
                "modality '{}' and type '{}' found:\n{}"
                .format(node.subject_id, node.visit_id, self.modality,
                        self.type, '\n'.join(
                            sorted(d.name for d in node.datasets))))
        return matches

    def __eq__(self, other):
        return (DatasetMatch.__eq__(self, other) and
                self.type == other.type and
                self.modality == other.modality and
                self.run == other.run)

    def __hash__(self):
        return (DatasetMatch.__hash__(self) ^
                hash(self.type) ^
                hash(self.modality) ^
                hash(self.run))

    def initkwargs(self):
        dct = DatasetMatch.initkwargs(self)
        dct['type'] = self.type
        dct['modality'] = self.modality
        dct['run'] = self.run
        return dct


class BidsAssociatedDatasetMatch(DatasetMatch):
    """
    A match object for matching BIDS datasets that are associated with
    another BIDS datasets (e.g. field-maps, bvecs, bvals)

    Parameters
    ----------
    name : str
        Name of the associated dataset
    primary_match : BidsDatasetMatch
        The primary dataset which the dataset to match is associated with
    associated : str
        The name of the association between the dataset to match and the
        primary dataset
    fieldmap_type : str
        Key of the return fieldmap dictionary (if association=='fieldmap'
    order : int
        Order of the fieldmap dictionary that you want to match
    """

    VALID_ASSOCIATIONS = ('fieldmap', 'bvec', 'bval')

    def __init__(self, name, primary_match, format, association,  # @ReservedAssignment @IgnorePep8
                 fieldmap_type=None, order=0):
        DatasetMatch.__init__(
            self, name, format, pattern=None, frequency='per_session',   # @ReservedAssignment @IgnorePep8
            derived=False, id=None, order=order, dicom_tags=None,
            is_regex=False, study=None)
        self._primary_match = primary_match
        self._association = association
        if fieldmap_type is not None and association != 'fieldmap':
            raise ArcanaUsageError(
                "'fieldmap_type' (provided to '{}' match) "
                "is only valid for 'fieldmap' "
                "associations (not '{}')".format(name, association))
        self._fieldmap_type = fieldmap_type

    def __repr__(self):
        return ("{}(name={}, primary_match={}, format={}, association={}, "
                "fieldmap_type\{}, order={})".format(
                    self.name, self.primary_match, self.format,
                    self.association, self.fieldmap_type,
                    self.order))

    @property
    def primary_match(self):
        return self._primary_match

    @property
    def association(self):
        return self._association

    @property
    def fieldmap_type(self):
        return self._fieldmap_type

    def _bind_node(self, node):
        primary_match = self._primary_match._bind_node(node)
        layout = self.study.repository.layout
        if self._association == 'fieldmap':
            matches = layout.get_fieldmap(primary_match.path,
                                          return_list=True)
            try:
                match = matches[0]
            except IndexError:
                raise ArcanaDatasetMatchError(
                    "Provided order to associated BIDS dataset match "
                    "{} is out of range")
        elif self._association == 'bvec':
            match = layout.get_bvec(primary_match.path)
        elif self._association == 'bval':
            match = layout.get_bval(primary_match.path)
        return match
        return matches

    def __eq__(self, other):
        return (DatasetMatch.__eq__(self, other) and
                self.primary_match == other.primary_match and
                self.association == other.association and
                self.fieldmap_type == other.fieldmap_type)

    def __hash__(self):
        return (DatasetMatch.__hash__(self) ^
                hash(self.primary_match) ^
                hash(self.association) ^
                hash(self.fieldmap_type))

    def initkwargs(self):
        dct = DatasetMatch.initkwargs(self)
        dct['primary_match'] = self.primary_match
        dct['association'] = self.association
        dct['fieldmap_type'] = self.fieldmap_type
        return dct
