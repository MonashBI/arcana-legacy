import os.path
import pydicom
from arcana.data_format import DataFormat, directory_format
from arcana.utils import split_extension
from arcana.exception import (
    ArcanaError, ArcanaDataFormatError,
    ArcanaDataFormatNotRegisteredError)
from .base import BaseDataset, BaseField


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
                raise ArcanaError(
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
                split_extension(f)[1] for f in os.listdir(path)
                if not f.startswith('.'))
            try:
                data_format = DataFormat.by_within_dir_exts(within_exts)
            except ArcanaDataFormatNotRegisteredError:
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
            raise ArcanaDataFormatError(
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
            raise ArcanaError(
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
