import os.path
import re
from abc import ABCMeta
from itertools import chain
import pydicom
from nianalysis.data_formats import DataFormat, directory_format
from copy import copy
from collections import defaultdict
from nianalysis.utils import split_extension
from logging import getLogger
from nianalysis.exceptions import (
    NiAnalysisError, NiAnalysisUsageError,
    NiAnalysisDatasetMatchError,
    NiAnalysisDataFormatError)

logger = getLogger('NiAnalysis')


class BaseDatum(object):

    MULTIPLICITY_OPTIONS = ('per_session', 'per_subject', 'per_visit',
                            'per_project')

    __metaclass__ = ABCMeta

    def __init__(self, name, frequency='per_session'):  # @ReservedAssignment @IgnorePep8
        assert name is None or isinstance(name, basestring)
        assert frequency in self.MULTIPLICITY_OPTIONS
        self._name = name
        self._frequency = frequency

    def __eq__(self, other):
        try:
            return (self.name == other.name and
                    self.frequency == other.frequency)
        except AttributeError as e:
            assert not e.message.startswith(
                "'{}'".format(self.__class__.__name__))
            return False

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}{t}('{}') != {t}('{}')".format(
                indent, self.name, other.name,
                t=type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if self.name != other.name:
            mismatch += ('\n{}name: self={} v other={}'
                         .format(sub_indent, self.name, other.name))
        if self.frequency != other.frequency:
            mismatch += ('\n{}frequency: self={} v other={}'
                         .format(sub_indent, self.frequency,
                                 other.frequency))
        return mismatch

    def __lt__(self, other):
        return self.name < other.name

    def __ne__(self, other):
        return not (self == other)

    def __iter__(self):
        return iter(self.to_tuple())

    @property
    def name(self):
        return self._name

    @property
    def frequency(self):
        return self._frequency

    def renamed(self, name):
        """
        Duplicate the datum and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate

    def initkwargs(self):
        return {'name': self.name,
                'frequency': self.frequency}


class BaseDataset(BaseDatum):
    """
    An abstract base class representing either an acquired dataset or the
    specification for a derived dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, format=None, frequency='per_session'):  # @ReservedAssignment @IgnorePep8
        super(BaseDataset, self).__init__(name=name, frequency=frequency)
        assert format is None or isinstance(format, DataFormat)
        self._format = format

    def __eq__(self, other):
        return (super(BaseDataset, self).__eq__(other) and
                self._format == other._format)

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseDataset, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.format != other.format:
            mismatch += ('\n{}format: self={} v other={}'
                         .format(sub_indent, self.format,
                                 other.format))
        return mismatch

    @property
    def format(self):
        return self._format

    def to_tuple(self):
        return (self.name, self.format.name, self.frequency, self.derived,
                self.is_spec)

    def match(self, filename):
        base, ext = os.path.splitext(filename)
        return base == self.name and (ext == self.format.extension or
                                      self.format is None)

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency={})"
                .format(self.__class__.__name__, self.name, self.format,
                        self.frequency))

    def initkwargs(self):
        dct = super(BaseDataset, self).initkwargs()
        dct['format'] = self.format
        return dct

    def fname(self, **kwargs):
        return self.basename(**kwargs) + self.format.ext_str


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
        cpy = copy(self)
        cpy._study = study
        cpy._match_tree(study.tree)
        return cpy

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
                raise NiAnalysisDatasetMatchError(
                    "Did not find {} datasets names matching pattern {}"
                    " (found {}) in {}".format(self.order, self.pattern,
                                               len(matches), node))
        elif len(matches) == 1:
            match = matches[0]
        elif matches:
            raise NiAnalysisDatasetMatchError(
                "Found multiple matches for {} pattern in {} ({})"
                .format(self.pattern, node,
                        ', '.join(str(m) for m in matches)))
        else:
            raise NiAnalysisDatasetMatchError(
                "Did not find any matches for {} pattern in {} "
                "(found {})"
                .format(self.pattern, node,
                        ', '.join(str(d) for d in node.datasets)))
        return match

    def match(self, subject_id=None, visit_id=None):
        if self._matches is None:
            raise NiAnalysisError(
                "{} has not been bound to study".format(self))
        if self.frequency == 'per_session':
            if subject_id is None or visit_id is None:
                raise NiAnalysisError(
                    "The 'subject_id' and 'visit_id' must be provided "
                    "to get the match from {}".format(self))
            dataset = self._matches[subject_id][visit_id]
        elif self.frequency == 'per_subject':
            if subject_id is None:
                raise NiAnalysisError(
                    "The 'subject_id' arg must be provided to get "
                    "the match from {}"
                    .format(self))
            dataset = self._matches[subject_id]
        elif self.frequency == 'per_visit':
            if visit_id is None:
                raise NiAnalysisError(
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
            raise NiAnalysisUsageError(
                "Either 'pattern' or 'id' need to be provided to "
                "DatasetMatch constructor")
        BaseDataset.__init__(self, name, format, frequency)
        BaseMatch.__init__(self, pattern, derived, order, study,
                           is_regex)
        if dicom_tags is not None and format.name != 'dicom':
            raise NiAnalysisUsageError(
                "Cannot use 'dicom_tags' kwarg with non-DICOM "
                "format ({})".format(format))
        self._dicom_tags = dicom_tags
        if order is not None and id is not None:
            raise NiAnalysisUsageError(
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
            raise NiAnalysisDatasetMatchError(
                "No dataset names in {} match '{}' pattern:{}"
                .format(node, self.pattern,
                        '\n'.join(d.name for d in node.datasets)))
        if self.id is not None:
            filtered = [d for d in matches if d.id == self.id]
            if not filtered:
                raise NiAnalysisDatasetMatchError(
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
                raise NiAnalysisDatasetMatchError(
                    "Did not find datasets names matching pattern {}"
                    "that matched DICOM tags {} (found {}) in {}"
                    .format(self.pattern, self.dicom_tags,
                            ', '.join(str(m) for m in matches), node))
            matches = filtered
        return matches


class DatasetSpec(BaseDataset):
    """
    A specification for a dataset within a study, which
    can either be an "acquired" dataset (e.g from the scanner)
    externally, or a "generated" dataset, derived from a processing
    pipeline.

    Parameters
    ----------
    name : str
        The name of the dataset
    format : FileFormat
        The file format used to store the dataset. Can be one of the
        recognised formats
    pipeline_name : str
        Name of the method in the study that is used to generate the
        dataset. If None the dataset is assumed to be acq
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    description : str
        Description of what the field represents
    """

    is_spec = True

    def __init__(self, name, format=None, pipeline_name=None,  # @ReservedAssignment @IgnorePep8
                 frequency='per_session', description=None):
        super(DatasetSpec, self).__init__(name, format, frequency)
        if not (pipeline_name is None or
                isinstance(pipeline_name, basestring)):
            raise NiAnalysisError(
                "Pipeline name for DatasetSpec '{}' is not a string "
                "'{}'".format(name, pipeline_name))
        self._pipeline_name = pipeline_name
        self._pipeline = None
        self._description = description
        self._study = None

    def __eq__(self, other):
        return (super(DatasetSpec, self).__eq__(other) and
                self.pipeline_name == other.pipeline_name and
                self._pipeline == other._pipeline and
                self.description == other.description and
                self._study == other._study)

    def bind(self, study):
        """
        Returns a copy of the DatasetSpec bound to the given study

        Parameters
        ----------
        study : Study
            A study to bind the dataset spec to (should happen in the
            study constructor)
        """
        cpy = copy(self)
        cpy._study = study
        if self.pipeline_name is not None:
            cpy.pipeline  # Test to see if pipeline name is present
        return cpy

    def find_mismatch(self, other, indent=''):
        mismatch = super(DatasetSpec, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.pipeline != other.pipeline:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, self.pipeline,
                                 other.pipeline))
        return mismatch

    @property
    def prefixed_name(self):
        return self.study.prefix + self.name

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def pipeline(self):
        try:
            return getattr(self.study, self.pipeline_name)
        except AttributeError:
            raise NiAnalysisError(
                "There is no pipeline method named '{}' in present in "
                "'{}' study".format(self.pipeline_name, self.study))

    @property
    def derived(self):
        return self.pipeline_name is not None

    @property
    def study(self):
        if self._study is None:
            raise NiAnalysisError(
                "{} is not bound to a study".format(self))
        return self._study

    @property
    def description(self):
        return self._description

    def basename(self, **kwargs):  # @UnusedVariable
        return self.prefixed_name

    def apply_prefix(self, prefix):
        """
        Duplicate the dataset and provide a prefix to apply to the filename
        """
        duplicate = copy(self)
        duplicate._prefix = prefix
        return duplicate

    def __repr__(self):
        return ("DatasetSpec(name='{}', format={}, pipeline_name={}, "
                "frequency={})".format(
                    self.name, self.format, self.pipeline_name,
                    self.frequency))

    def initkwargs(self):
        dct = super(DatasetSpec, self).initkwargs()
        dct['pipeline_name'] = self.pipeline_name
        dct['description'] = self.description
        return dct


class Dataset(BaseDataset):
    """
    A representation of a dataset within the archive.

    Parameters
    ----------
    name : str
        The name of the dataset
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
    path : str | None
        The path to the dataset (for archives on the local system)
    id : int | None
        The ID of the dataset in the session. To be used to
        distinguish multiple datasets with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        archives where this isn't stored (i.e. Local), id can be None
    subject_id : int | str | None
        The id of the subject which the dataset belongs to
    visit_id : int | str | None
        The id of the visit which the dataset belongs to
    archive : BaseArchive
        The archive which the dataset is stored
    """

    is_spec = False

    def __init__(self, name, format=None, derived=False,  # @ReservedAssignment @IgnorePep8
                 frequency='per_session', path=None,
                 id=None, uri=None, subject_id=None, visit_id=None,  # @ReservedAssignment @IgnorePep8
                 archive=None):
        super(Dataset, self).__init__(name, format, frequency)
        self._derived = derived
        self._path = path
        self._id = id
        self._uri = uri
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._archive = archive

    def __eq__(self, other):
        return (super(Dataset, self).__eq__(other) and
                self.derived == other.derived and
                self._path == other._path and
                self.id == other.id and
                self.subject_id == other.subject_id and
                self.visit_id == other.visit_id and
                self._archive == other._archive)

    def __lt__(self, other):
        return self.id < other.id

    def find_mismatch(self, other, indent=''):
        mismatch = super(Dataset, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.derived != other.derived:
            mismatch += ('\n{}derived: self={} v other={}'
                         .format(sub_indent, self.derived,
                                 other.derived))
        if self._path != other._path:
            mismatch += ('\n{}path: self={} v other={}'
                         .format(sub_indent, self._path,
                                 other._path))
        if self._id != other._id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self._id,
                                 other._id))
        if self.subject_id != other.subject_id:
            mismatch += ('\n{}subject_id: self={} v other={}'
                         .format(sub_indent, self.subject_id,
                                 other.subject_id))
        if self.visit_id != other.visit_id:
            mismatch += ('\n{}visit_id: self={} v other={}'
                         .format(sub_indent, self.visit_id,
                                 other.visit_id))
        if self.archive != other.archive:
            mismatch += ('\n{}archive: self={} v other={}'
                         .format(sub_indent, self.archive,
                                 other.archive))
        return mismatch

    @property
    def path(self):
        if self._path is None:
            if self.archive is not None:
                self._path = self.archive.cache(self)
            else:
                raise NiAnalysisError(
                    "Neither path nor archive has been set for Dataset "
                    "{}".format(self.name))
        return self._path

    def basename(self, **kwargs):  # @UnusedVariable
        return self.name

    @property
    def derived(self):
        return self._derived

    @property
    def id(self):
        if self._id is None:
            return self.name
        else:
            return self._id

    @property
    def uri(self):
        return self._uri

    @property
    def archive(self):
        return self._archive

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def visit_id(self):
        return self._visit_id

    @classmethod
    def from_path(cls, path, frequency='per_session',
                  subject_id=None, visit_id=None, archive=None):
        if os.path.isdir(path):
            within_exts = frozenset(
                split_extension(f)[1] for f in os.listdir(path))
            try:
                data_format = DataFormat.by_within_dir_exts(within_exts)
            except KeyError:
                # Fall back to general directory format
                data_format = directory_format
            name = os.path.basename(path)
        else:
            filename = os.path.basename(path)
            name, ext = split_extension(filename)
            data_format = DataFormat.by_ext(ext)
        return cls(name, data_format, frequency=frequency,
                   path=path, derived=False, subject_id=subject_id,
                   visit_id=visit_id, archive=archive)

    def dicom(self, index):
        """
        Returns a PyDicom object for the DICOM file at index 'index'

        Parameters
        ----------
        dataset : Dataset
            The dataset to read a DICOM file from
        index : int
            The index of the DICOM file in the dataset to read

        Returns
        -------
        dcm : pydicom.DICOM
            A PyDicom file object
        """
        if self.format.name != 'dicom':
            raise NiAnalysisDataFormatError(
                "Can not read DICOM header as {} is not in DICOM format"
                .format(self))
        fnames = sorted(os.listdir(self.path))
        with open(os.path.join(self.path, fnames[index])) as f:
            dcm = pydicom.dcmread(f)
        return dcm

    def dicom_values(self, tags, archive_login=None):
        """
        Returns a dictionary with the DICOM header fields corresponding
        to the given tag names

        Parameters
        ----------
        tags : List[Tuple[str, str]]
            List of DICOM tag values as 2-tuple of strings, e.g.
            [('0080', '0020')]
        archive_login : <archive-login-object>
            A login object for the archive to avoid having to relogin
            for every dicom_header call.

        Returns
        -------
        dct : Dict[Tuple[str, str], str|int|float]
        """
        if (self._path is None and self._archive is not None and
                hasattr(self.archive, 'dicom_header')):
            hdr = self.archive.dicom_header(self,
                                            prev_login=archive_login)
            dct = {t: hdr[t] for t in tags}
        else:
            # Get the DICOM object for the first file in the dataset
            dcm = self.dicom(0)
            dct = {t: dcm[t].value for t in tags}
        return dct

    def initkwargs(self):
        dct = super(Dataset, self).initkwargs()
        dct['derived'] = self.derived
        dct['path'] = self.path
        dct['id'] = self.id
        dct['uri'] = self.uri
        dct['archive'] = self.archive
        return dct


class BaseField(BaseDatum):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived value.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    """

    __metaclass__ = ABCMeta

    dtypes = (int, float, str)

    def __init__(self, name, dtype, frequency):
        super(BaseField, self).__init__(name, frequency)
        if dtype not in self.dtypes:
            raise NiAnalysisError(
                "Invalid dtype {}, can be one of {}".format(
                    dtype.__name__, ', '.join(self._dtype_names())))
        self._dtype = dtype

    def __eq__(self, other):
        return (super(BaseField, self).__eq__(other) and
                self.dtype == other.dtype)

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseField, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.dtype != other.dtype:
            mismatch += ('\n{}dtype: self={} v other={}'
                         .format(sub_indent, self.dtype,
                                 other.dtype))
        return mismatch

    @property
    def dtype(self):
        return self._dtype

    @classmethod
    def _dtype_names(cls):
        return (d.__name__ for d in cls.dtypes)

    def initkwargs(self):
        dct = super(BaseField, self).initkwargs()
        dct['dtype'] = self.dtype
        return dct


class Field(BaseField):
    """
    A representation of a value field in the archive.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    derived : bool
        Whether or not the value belongs in the derived session or not
    subject_id : int | str | None
        The id of the subject which the field belongs to
    visit_id : int | str | None
        The id of the visit which the field belongs to
    archive : BaseArchive
        The archive which the field is stored
    """

    def __init__(self, name, value, frequency='per_session',
                 derived=False, subject_id=None, visit_id=None,
                 archive=None):
        if isinstance(value, int):
            dtype = int
        elif isinstance(value, float):
            dtype = float
        elif isinstance(value, basestring):
            # Attempt to implicitly convert from string
            try:
                value = int(value)
                dtype = int
            except ValueError:
                try:
                    value = float(value)
                    dtype = float
                except ValueError:
                    dtype = str
        else:
            raise NiAnalysisError(
                "Unrecognised field dtype {} (can be int, float or str)"
                .format(value))
        super(Field, self).__init__(
            name, dtype, frequency=frequency)
        self._value = value
        self._derived = derived
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._archive = archive

    def __eq__(self, other):
        return (super(Field, self).__eq__(other) and
                self.derived == other.derived and
                self.value == other.value and
                self.subject_id == other.subject_id and
                self.visit_id == other.visit_id and
                self._archive == other._archive)

    def find_mismatch(self, other, indent=''):
        mismatch = super(Field, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.derived != other.derived:
            mismatch += ('\n{}derived: self={} v other={}'
                         .format(sub_indent, self.derived,
                                 other.derived))
        if self.value != other.value:
            mismatch += ('\n{}value: self={} v other={}'
                         .format(sub_indent, self.value,
                                 other.value))
        if self.subject_id != other.subject_id:
            mismatch += ('\n{}subject_id: self={} v other={}'
                         .format(sub_indent, self.subject_id,
                                 other.subject_id))
        if self.visit_id != other.visit_id:
            mismatch += ('\n{}visit_id: self={} v other={}'
                         .format(sub_indent, self.visit_id,
                                 other.visit_id))
        if self.archive != other.archive:
            mismatch += ('\n{}archive: self={} v other={}'
                         .format(sub_indent, self.archive,
                                 other.archive))
        return mismatch

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return ("{}(name='{}', value={}, frequency='{}', derived={},"
                " subject_id={}, visit_id={}, archive={})".format(
                    type(self).__name__, self.name, self.value,
                    self.frequency, self.derived, self.subject_id,
                    self.visit_id, self.archive))

    @property
    def derived(self):
        return self._derived

    def basename(self, **kwargs):  # @UnusedVariable
        return self.name

    @property
    def archive(self):
        return self._archive

    @property
    def value(self):
        return self._value

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def visit_id(self):
        return self._visit_id

    def initkwargs(self):
        dct = {}
        dct['name'] = self.name
        dct['value'] = self.value
        dct['frequency'] = self.frequency
        dct['derived'] = self.derived
        dct['archive'] = self.archive
        return dct


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

    def __init__(self, name, pattern, dtype, frequency='per_session',
                 derived=False, order=None, is_regex=False, study=None):
        FieldMatch.__init__(self, name, format, frequency)
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
            raise NiAnalysisDatasetMatchError(
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


class FieldSpec(BaseField):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived dataset.

    Parameters
    ----------
    name : str
        The name of the dataset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    pipeline : method
        Method that generates values for the specified field.
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_project',
        specifying whether the dataset is present for each session, subject,
        visit or project.
    description : str
        Description of what the field represents
    """

    is_spec = True

    def __init__(self, name, dtype, pipeline=None,
                 frequency='per_session', description=None):
        super(FieldSpec, self).__init__(name, dtype, frequency)
        self._pipeline = pipeline
        self._description = description
        self._prefix = ''

    def __eq__(self, other):
        return (super(FieldSpec, self).__eq__(other) and
                self.pipeline == other.pipeline)

    def find_mismatch(self, other, indent=''):
        mismatch = super(FieldSpec, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.pipeline != other.pipeline:
            mismatch += ('\n{}pipeline: self={} v other={}'
                         .format(sub_indent, self.pipeline,
                                 other.pipeline))
        return mismatch

    @property
    def prefixed_name(self):
        return self._prefix + self.name

    def basename(self, **kwargs):  # @UnusedVariable
        return self._prefix + self.name

    @property
    def dtype(self):
        return self._dtype

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def derived(self):
        return self._pipeline is not None

    @property
    def description(self):
        return self._description

    def apply_prefix(self, prefix):
        """
        Duplicate the field and provide a prefix to apply to the name
        """
        duplicate = copy(self)
        duplicate._prefix = prefix
        return duplicate

    def __repr__(self):
        return ("{}(name='{}', dtype={}, pipeline={}, "
                "frequency={})".format(
                    self.__class__.__name__, self.name, self.dtype,
                    self.pipeline, self.frequency))

    def initkwargs(self):
        dct = super(FieldSpec, self).initkwargs()
        dct['pipeline'] = self.pipeline
        dct['description'] = self.description
        return dct
