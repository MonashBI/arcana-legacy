from builtins import str
from past.builtins import basestring
import os.path
import pydicom
from arcana.dataset.file_format import FileFormat
from arcana.dataset.file_format.standard import directory_format
from arcana.utils import split_extension
from arcana.exception import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError,
    ArcanaFileFormatNotRegisteredError, ArcanaNameError)
from .base import BaseDataset, BaseField

DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class BaseItem(object):

    is_spec = False

    def __init__(self, subject_id, visit_id, repository, from_study):
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._repository = repository
        self._from_study = from_study

    def __eq__(self, other):
        return (self.subject_id == other.subject_id and
                self.visit_id == other.visit_id and
                self.from_study == other.from_study)

    def __hash__(self):
        return (hash(self.subject_id) ^
                hash(self.visit_id) ^
                hash(self.from_study))

    def find_mismatch(self, other, indent=''):
        sub_indent = indent + '  '
        mismatch = ''
        if self.subject_id != other.subject_id:
            mismatch += ('\n{}subject_id: self={} v other={}'
                         .format(sub_indent, self.subject_id,
                                 other.subject_id))
        if self.visit_id != other.visit_id:
            mismatch += ('\n{}visit_id: self={} v other={}'
                         .format(sub_indent, self.visit_id,
                                 other.visit_id))
        if self.from_study != other.from_study:
            mismatch += ('\n{}from_study: self={} v other={}'
                         .format(sub_indent, self.from_study,
                                 other.from_study))
        return mismatch

    @property
    def derived(self):
        return self.from_study is not None

    @property
    def repository(self):
        return self._repository

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def from_study(self):
        return self._from_study

    def initkwargs(self):
        dct = super(Dataset, self).initkwargs()
        dct['repository'] = self.repository
        dct['subject_id'] = self.subject_id
        dct['visit_id'] = self.visit_id
        dct['from_study'] = self._from_study
        return dct


class Dataset(BaseItem, BaseDataset):
    """
    A representation of a dataset within the repository.

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
        Whether the scan was generated or acquired. Depending on the repository
        used to store the dataset this is used to determine the location of the
        dataset.
    path : str | None
        The path to the dataset (for repositorys on the local system)
    id : int | None
        The ID of the dataset in the session. To be used to
        distinguish multiple datasets with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        repositorys where this isn't stored (i.e. Local), id can be None
    subject_id : int | str | None
        The id of the subject which the dataset belongs to
    visit_id : int | str | None
        The id of the visit which the dataset belongs to
    repository : BaseRepository
        The repository which the dataset is stored
    from_study : str
        Name of the Arcana study that that generated the field
    """

    def __init__(self, name, format=None, frequency='per_session', # @ReservedAssignment @IgnorePep8
                 path=None, id=None, uri=None, subject_id=None, # @ReservedAssignment @IgnorePep8
                 visit_id=None, repository=None, from_study=None,
                 bids_attr=None):
        BaseDataset.__init__(self, name=name, format=format,
                             frequency=frequency)
        BaseItem.__init__(self, subject_id, visit_id, repository,
                          from_study)
        self._path = path
        self._uri = uri
        self._bids_attr = bids_attr
        if id is None and path is not None and format.name == 'dicom':
            self._id = int(self.dicom_values([DICOM_SERIES_NUMBER_TAG])
                           [DICOM_SERIES_NUMBER_TAG])
        else:
            self._id = id

    def __eq__(self, other):
        return (BaseDataset.__eq__(self, other) and
                BaseItem.__eq__(self, other) and
                self._path == other._path and
                self.id == other.id and
                self._bids_attr == other._bids_attr)

    def __hash__(self):
        return (BaseDataset.__hash__(self) ^
                BaseItem.__hash__(self) ^
                hash(self._path) ^
                hash(self.id) ^
                hash(self._bids_attr))

    def __lt__(self, other):
        if isinstance(self.id, int) and isinstance(other.id, str):
            return True
        elif isinstance(self.id, str) and isinstance(other.id, int):
            return False
        else:
            if self.id == other.id:
                # If ids are equal order depending on study name
                # with acquired (from_study==None) coming first
                if self.from_study is None:
                    return other.from_study is None
                elif other.from_study is None:
                    return False
                else:
                    return self.from_study < other.from_study
            else:
                return self.id < other.id

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency='{}', "
                "subject_id={}, visit_id={}, from_study={})".format(
                    type(self).__name__, self.name, self.format,
                    self.frequency, self.subject_id,
                    self.visit_id, self.from_study))

    @property
    def fname(self):
        return self.name + self.format.ext_str

    @property
    def bids_attr(self):
        return self._bids_attr

    def find_mismatch(self, other, indent=''):
        mismatch = BaseDataset.find_mismatch(self, other, indent)
        mismatch += BaseItem.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self._path != other._path:
            mismatch += ('\n{}path: self={} v other={}'
                         .format(sub_indent, self._path,
                                 other._path))
        if self._id != other._id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self._id,
                                 other._id))
        if self._bids_attr != other._bids_attr:
            mismatch += ('\n{}bids_attr: self={} v other={}'
                         .format(sub_indent, self._bids_attr,
                                 other._bids_attr))
        return mismatch

    @property
    def path(self):
        if self._path is None:
            if self.repository is not None:
                self._path = self.repository.get_dataset(self)
            else:
                raise ArcanaError(
                    "Neither path nor repository has been set for Dataset "
                    "{}".format(self.name))
        return self._path

    @path.setter
    def path(self, path):
        self._path = path

    def basename(self, **kwargs):  # @UnusedVariable
        return self.name

    @property
    def id(self):
        if self._id is None:
            return self.name
        else:
            return self._id

    @property
    def uri(self):
        return self._uri

    @classmethod
    def from_path(cls, path, frequency='per_session', format=None,  # @ReservedAssignment @IgnorePep8
                  **kwargs):
        if not os.path.exists(path):
            raise ArcanaUsageError(
                "Attempting to read Dataset from path '{}' but it "
                "does not exist".format(path))
        if os.path.isdir(path):
            within_exts = frozenset(
                split_extension(f)[1] for f in os.listdir(path)
                if not f.startswith('.'))
            if format is None:
                # Try to guess format
                try:
                    format = FileFormat.by_within_dir_exts(within_exts)  # @ReservedAssignment @IgnorePep8
                except ArcanaFileFormatNotRegisteredError:
                    # Fall back to general directory format
                    format = directory_format  # @ReservedAssignment
            name = os.path.basename(path)
        else:
            filename = os.path.basename(path)
            name, ext = split_extension(filename)
            if format is None:
                try:
                    format = FileFormat.by_ext(ext)  # @ReservedAssignment @IgnorePep8
                except ArcanaFileFormatNotRegisteredError as e:
                    raise ArcanaFileFormatNotRegisteredError(
                        str(e) + ", which is required to identify the "
                        "format of the dataset at '{}'".format(path))
        return cls(name, format, frequency=frequency,
                   path=path, **kwargs)

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
            raise ArcanaFileFormatError(
                "Can not read DICOM header as {} is not in DICOM format"
                .format(self))
        fnames = sorted([f for f in os.listdir(self.path)
                         if not f.startswith('.')])
        with open(os.path.join(self.path, fnames[index]), 'rb') as f:
            dcm = pydicom.dcmread(f)
        return dcm

    def dicom_values(self, tags):
        """
        Returns a dictionary with the DICOM header fields corresponding
        to the given tag names

        Parameters
        ----------
        tags : List[Tuple[str, str]]
            List of DICOM tag values as 2-tuple of strings, e.g.
            [('0080', '0020')]
        repository_login : <repository-login-object>
            A login object for the repository to avoid having to relogin
            for every dicom_header call.

        Returns
        -------
        dct : Dict[Tuple[str, str], str|int|float]
        """
        try:
            if (self._path is None and self._repository is not None and
                    hasattr(self.repository, 'dicom_header')):
                hdr = self.repository.dicom_header(self)
                dct = {t: hdr[t] for t in tags}
            else:
                # Get the DICOM object for the first file in the dataset
                dcm = self.dicom(0)
                dct = {t: dcm[t].value for t in tags}
        except KeyError as e:
            raise ArcanaNameError(
                e.args[0], "{} does not have dicom tag {}".format(
                    self, e.args[0]))
        return dct

    def initkwargs(self):
        dct = BaseDataset.initkwargs(self)
        dct.update(BaseItem.initkwargs(self))
        dct['path'] = self.path
        dct['id'] = self.id
        dct['uri'] = self.uri
        dct['bids_attr'] = self.bids_attr
        return dct

    def get(self):
        if self.repository is not None:
            self._value = self.repository.get_dataset(self)

    def put(self):
        if self.repository is not None:
            self.repository.put_dataset(self)


class Field(BaseItem, BaseField):
    """
    A representation of a value field in the repository.

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
    repository : BaseRepository
        The repository which the field is stored
    from_study : str
        Name of the Arcana study that that generated the field
    """

    def __init__(self, name, value=None, dtype=None,
                 frequency='per_session', subject_id=None,
                 visit_id=None, repository=None, from_study=None):
        if dtype is None:
            if value is None:
                raise ArcanaUsageError(
                    "Either 'value' or 'dtype' must be provided to "
                    "Field init")
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
                raise ArcanaUsageError(
                    "Unrecognised field dtype {} (can be int, float or"
                    " str)".format(value))
        else:
            if value is not None:
                value = dtype(value)
        BaseField.__init__(self, name, dtype, frequency)
        BaseItem.__init__(self, subject_id, visit_id, repository,
                          from_study)
        self._value = value

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseItem.__eq__(self, other) and
                self.value == other.value)

    def __hash__(self):
        return (BaseField.__hash__(self) ^
                BaseItem.__hash__(self) ^
                hash(self.value))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseField.find_mismatch(self, other, indent)
        mismatch += BaseItem.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.value != other.value:
            mismatch += ('\n{}value: self={} v other={}'
                         .format(sub_indent, self.value,
                                 other.value))
        return mismatch

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        return str(self.value)

    def __lt__(self, other):
        if self.name == other.name:
            # If ids are equal order depending on study name
            # with acquired (from_study==None) coming first
            if self.from_study is None:
                return other.from_study is None
            elif other.from_study is None:
                return False
            else:
                return self.from_study < other.from_study
        else:
            return self.name < other.name

    def __repr__(self):
        return ("{}(name='{}', value={}, frequency='{}',  "
                "subject_id={}, visit_id={}, from_study={})".format(
                    type(self).__name__, self.name, self.value,
                    self.frequency, self.subject_id,
                    self.visit_id, self.from_study))

    def basename(self, **kwargs):  # @UnusedVariable
        return self.name

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = self.dtype(value)

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseItem.initkwargs(self))
        dct['value'] = self.value
        return dct

    def get(self):
        if self.repository is not None:
            self._value = self.repository.get_field(self)

    def put(self):
        if self.repository is not None:
            self.repository.put_field(self)