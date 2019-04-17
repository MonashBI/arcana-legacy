from past.builtins import basestring
import os
from itertools import chain
from collections.abc import Iterable
from collections import defaultdict
from copy import copy
import os.path as op
import hashlib
import pydicom
from arcana.utils import split_extension, parse_value
from arcana.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError, ArcanaNameError,
    ArcanaDataNotDerivedYetError)
from .base import BaseFileset, BaseField

DICOM_SERIES_NUMBER_TAG = ('0020', '0011')


class BaseItem(object):

    is_spec = False

    def __init__(self, subject_id, visit_id, repository, from_study,
                 exists, record):
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._repository = repository
        self._from_study = from_study
        self._exists = exists
        self._record = record

    def __eq__(self, other):
        return (self.subject_id == other.subject_id and
                self.visit_id == other.visit_id and
                self.from_study == other.from_study and
                self.exists == other.exists and
                self._record == other._record)

    def __hash__(self):
        return (hash(self.subject_id) ^
                hash(self.visit_id) ^
                hash(self.from_study) ^
                hash(self.exists) ^
                hash(self._record))

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
        if self.exists != other.exists:
            mismatch += ('\n{}exists: self={} v other={}'
                         .format(sub_indent, self.exists,
                                 other.exists))
        if self._record != other._record:
            mismatch += ('\n{}_record: self={} v other={}'
                         .format(sub_indent, self._record,
                                 other._record))
        return mismatch

    @property
    def derived(self):
        return self.from_study is not None

    @property
    def repository(self):
        return self._repository

    @property
    def exists(self):
        return self._exists

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def session_id(self):
        return (self.subject_id, self.visit_id)

    @property
    def from_study(self):
        return self._from_study

    @property
    def record(self):
        return self._record

    @record.setter
    def record(self, record):
        if self.name not in record.outputs:
            raise ArcanaNameError(
                self.name,
                "{} was not found in outputs {} of provenance record".format(
                    self.name, record.outputs.keys(), record))
        self._record = record

    @property
    def recorded_checksums(self):
        if self.record is None:
            return None
        else:
            return self._record.outputs[self.name]

    def initkwargs(self):
        dct = super(Fileset, self).initkwargs()
        dct['repository'] = self.repository
        dct['subject_id'] = self.subject_id
        dct['visit_id'] = self.visit_id
        dct['from_study'] = self._from_study
        return dct


class Fileset(BaseItem, BaseFileset):
    """
    A representation of a fileset within the repository.

    Parameters
    ----------
    name : str
        The name of the fileset
    format : FileFormat
        The file format used to store the fileset.
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    derived : bool
        Whether the scan was generated or acquired. Depending on the repository
        used to store the fileset this is used to determine the location of the
        fileset.
    path : str | None
        The path to the fileset (for repositories on the local system)
    aux_files : dict[str, str] | None
        Additional files in the fileset. Keys should match corresponding
        aux_files dictionary in format.
    id : int | None
        The ID of the fileset in the session. To be used to
        distinguish multiple filesets with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        repositorys where this isn't stored (i.e. Local), id can be None
    subject_id : int | str | None
        The id of the subject which the fileset belongs to
    visit_id : int | str | None
        The id of the visit which the fileset belongs to
    repository : Repository
        The repository which the fileset is stored
    from_study : str
        Name of the Arcana study that that generated the field
    exists : bool
        Whether the fileset exists or is just a placeholder for a derivative
    checksums : dict[str, str]
        A checksums of all files within the fileset in a dictionary sorted by
        relative file paths
    record : arcana.pipeline.provenance.Record | None
        The provenance record for the pipeline that generated the file set,
        if applicable
    format_name : str | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT) the format name can be recorded to aid format
        identification
    potential_aux_files : list[str]
        A list of paths to potential files to include in the fileset as
        "side-cars" or headers or in a directory format. Used when the
        format of the fileset is not set when it is detected in the repository
        but determined later from a list of candidates in the specification it
        is matched to.
    """

    def __init__(self, name, format=None, frequency='per_session', # @ReservedAssignment @IgnorePep8
                 path=None, aux_files=None, id=None, uri=None, subject_id=None, # @ReservedAssignment @IgnorePep8
                 visit_id=None, repository=None, from_study=None,
                 exists=True, checksums=None, record=None, format_name=None,
                 potential_aux_files=None):
        BaseFileset.__init__(self, name=name, format=format,
                             frequency=frequency)
        BaseItem.__init__(self, subject_id, visit_id, repository,
                          from_study, exists, record)
        if aux_files is not None:
            if path is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' fileset ({}) but not primary "
                    "path".format(self.name, aux_files))
            if format is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' fileset ({}) but format is "
                    "not specified".format(self.name, aux_files))
        if path is not None:
            path = op.abspath(op.realpath(path))
            if aux_files is None:
                aux_files = {}
            if set(aux_files.keys()) != set(self.format.aux_files.keys()):
                raise ArcanaUsageError(
                    "Provided side cars for '{}' but expected '{}'"
                    .format("', '".join(aux_files.keys()),
                            "', '".join(self.format.aux_files.keys())))
        self._path = path
        self._aux_files = aux_files
        self._uri = uri
        if id is None and path is not None and format.name == 'dicom':
            self._id = int(self.dicom_values([DICOM_SERIES_NUMBER_TAG])
                           [DICOM_SERIES_NUMBER_TAG])
        else:
            self._id = id
        self._checksums = checksums
        self._format_name = format_name
        if potential_aux_files is not None and format is not None:
            raise ArcanaUsageError(
                "Potential paths should only be provided to Fileset.__init__ "
                "({}) when the format of the fileset ({}) is not determined"
                .format(self.name, format))
        if potential_aux_files is not None:
            potential_aux_files = list(potential_aux_files)
        self._potential_aux_files = potential_aux_files

    def __getattr__(self, attr):
        """
        For the convenience of being able to make calls on a fileset that are
        dependent on its format, e.g.

            >>> fileset = Fileset('a_name', format=AnImageFormat())
            >>> fileset.get_header()

        we capture missing attributes and attempt to redirect them to methods
        of the format class that take the fileset as the first argument
        """
        try:
            frmt = self.__dict__['_format']
        except KeyError:
            frmt = None
        else:
            try:
                format_attr = getattr(frmt, attr)
            except AttributeError:
                pass
            else:
                if callable(format_attr):
                    return lambda *args, **kwargs: format_attr(self, *args,
                                                               **kwargs)
        raise AttributeError("Filesets of {} format don't have a '{}' "
                             "attribute".format(frmt, attr))

    def __eq__(self, other):
        eq = (BaseFileset.__eq__(self, other) and
              BaseItem.__eq__(self, other) and
              self.aux_files == other.aux_files and
              self.id == other.id and
              self.checksums == other.checksums and
              self.format_name == other.format_name)
        # Avoid having to cache fileset in order to test equality unless they
        # are already both cached
        try:
            if self._path is not None and other._path is not None:
                eq &= (self._path == other._path)
        except AttributeError:
            return False
        return eq

    def __hash__(self):
        return (BaseFileset.__hash__(self) ^
                BaseItem.__hash__(self) ^
                hash(self.id) ^
                hash(tuple(sorted(self.aux_files.items()))) ^
                hash(self.checksums) ^
                hash(self.format_name))

    def __lt__(self, other):
        if isinstance(self.id, int) and isinstance(other.id, basestring):
            return True
        elif isinstance(self.id, basestring) and isinstance(other.id, int):
            return False
        else:
            if self.id == other.id:
                # If ids are equal order depending on study name
                # with acquired (from_study==None) coming first
                if self.from_study is None:
                    return other.from_study is not None
                elif other.from_study is None:
                    return False
                elif self.from_study == other.from_study:
                    if self.format_name is None:
                        return other.format_name is not None
                    elif other.format_name is None:
                        return False
                    else:
                        return self.format_name < other.format_name
                else:
                    return self.from_study < other.from_study
            else:
                return self.id < other.id

    def __repr__(self):
        return ("{}('{}', {}, '{}', subj={}, vis={}, stdy={}{}, exists={}{})"
                .format(
                    type(self).__name__, self.name, self.format,
                    self.frequency, self.subject_id,
                    self.visit_id, self.from_study,
                    (", format_name='{}'" if self._format_name is not None
                     else ''),
                    self.exists,
                    (", path='{}'".format(self.path)
                     if self._path is not None else '')))

    def find_mismatch(self, other, indent=''):
        mismatch = BaseFileset.find_mismatch(self, other, indent)
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
        if self.checksums != other.checksums:
            mismatch += ('\n{}checksum: self={} v other={}'
                         .format(sub_indent, self.checksums,
                                 other.checksums))
        if self._format_name != other._format_name:
            mismatch += ('\n{}format_name: self={} v other={}'
                         .format(sub_indent, self._format_name,
                                 other._format_name))
        return mismatch

    @property
    def path(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name,
                "Cannot access path of {} as it hasn't been derived yet"
                .format(self))
        if self._path is None:
            if self.repository is not None:
                self.get()  # Retrieve from repository
            else:
                raise ArcanaError(
                    "Neither path nor repository has been set for Fileset("
                    "'{}')".format(self.name))
        return self._path

    def set_path(self, path, aux_files=None):
        if path is not None:
            path = op.abspath(op.realpath(path))
            self._exists = True
        self._path = path
        if aux_files is None:
            self._aux_files = dict(self.format.aux_file_paths(path))
        else:
            if set(self.format.aux_files.keys()) != set(aux_files.keys()):
                raise ArcanaUsageError(
                    "Keys of provided side cars ('{}') don't match format "
                    "('{}')".format("', '".join(aux_files.keys()),
                                    "', '".join(self.format.aux_files.keys())))
            self._aux_files = aux_files
        self._checksums = self.calculate_checksums()
        self.put()  # Push to repository

    @path.setter
    def path(self, path):
        self.set_path(path, aux_files=None)

    @property
    def paths(self):
        """Iterates through all files in the set"""
        if self.format.directory:
            return chain(*((op.join(root, f) for f in files)
                           for root, _, files in os.walk(self.path)))
        else:
            return chain([self.path], self.aux_files.values())

    @property
    def fname(self):
        if not self.name.endswith(self.format.ext_str):
            fname = self.name + self.format.ext_str
        else:
            fname = self.name
        return fname

    @property
    def basename(self):
        if self.format.ext_str and self.name.endswith(self.format.ext_str):
            basename = self.name[:-len(self.format.ext_str)]
        else:
            basename = self.name
        return basename

    @property
    def id(self):
        if self._id is None:
            return self.basename
        else:
            return self._id

    @id.setter
    def id(self, id):  # @ReservedAssignment
        if self._id is None:
            self._id = id
        elif id != self._id:
            raise ArcanaUsageError("Can't change value of ID for {} from {} "
                                   "to {}".format(self, self._id, id))

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        if self._uri is None:
            self._uri = uri
        elif uri != self._uri:
            raise ArcanaUsageError("Can't change value of URI for {} from {} "
                                   "to {}".format(self, self._uri, uri))

    def aux_file(self, name):
        return self.aux_files[name]

    @property
    def aux_files(self):
        return self._aux_files if self._aux_files is not None else {}

    @property
    def aux_file_fnames_and_paths(self):
        return ((self.basename + self.format.aux_files[sc_name], sc_path)
                for sc_name, sc_path in self.aux_files.items())

    @property
    def format_name(self):
        if self.format is None:
            name = self._format_name
        else:
            name = self.format.name
        return name

    @property
    def checksums(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name,
                "Cannot access checksums of {} as it hasn't been derived yet"
                .format(self))
        if self._checksums is None:
            if self.repository is not None:
                self._checksums = self.repository.get_checksums(self)
            if self._checksums is None:
                self._checksums = self.calculate_checksums()
        return self._checksums

    def calculate_checksums(self):
        checksums = {}
        for fpath in self.paths:
            with open(fpath, 'rb') as f:
                checksums[op.relpath(fpath, self.path)] = hashlib.md5(
                    f.read()).hexdigest()
        return checksums

    @classmethod
    def from_path(cls, path, from_study=None, **kwargs):
        if not op.exists(path):
            raise ArcanaUsageError(
                "Attempting to read Fileset from path '{}' but it "
                "does not exist".format(path))
        if op.isdir(path):
            name = op.basename(path)
        else:
            filename = op.basename(path)
            basename = split_extension(filename)[0]
            if from_study is None:
                # For acquired datasets we can't be sure that the name is
                # unique within the directory if we strip the extension so we
                # need to keep it in
                name = filename
            else:
                name = basename
            # Create side cars dictionary from default extensions
        return cls(name, from_study=from_study, **kwargs)

    def select_format(self, candidates):
        """
        Selects a single matching format of the fileset from a list of possible
        candidates. If multiple candidates match the potential files, e.g.
        NiFTI-X (see dcm2niix) and NiFTI, then the first matching candidate is
        selected.

        If a 'format_name' was specified when the fileset was
        created then that is used to select between the candidates. Otherwise
        the file extensions of the primary path and potential auxiliary files,
        or extensions of the files within the directory for directories are
        matched against those specified for the file formats

        Parameters
        ----------
        candidates : FileFormat
            A list of file-formats to select from.
        """
        if self._format_name is not None:
            matches = [c for c in candidates if c.name == self._format_name]
            if not matches:
                raise ArcanaFileFormatError(
                    "None of the candidate file formats ({}) match the "
                    "saved format name '{}' of {}"
                    .format(', '.join(str(c) for c in candidates),
                            self._format_name, {}))
            elif len(matches) > 1:
                raise ArcanaFileFormatError(
                    "Multiple candidate file formats ({}) match saved format "
                    "name {}".format(', '.join(str(m) for m in matches), self))
        else:
            if op.isdir(self.path):
                # Get set of all extensions in the directory
                within_exts = frozenset(
                    split_extension(f)[1] for f in os.listdir(self.path)
                    if not f.startswith('.'))
                matches = [c for c in candidates
                           if c.within_dir_exts == within_exts]
                if not matches:
                    raise ArcanaFileFormatError(
                        "None of the candidate file formats ({}) match the "
                        "file extensions within {} ({})"
                        .format(', '.join(str(c) for c in candidates),
                                self, within_exts))
            else:
                matches = []
                all_paths = [self.path] + self._potential_aux_files
                for candidate in candidates:
                    try:
                        primary_path = candidate.select_files(all_paths)[0]
                    except ArcanaFileFormatError:
                        continue
                    else:
                        if primary_path == self.path:
                            matches.append(candidate)
                if not matches:
                    raise ArcanaFileFormatError(
                        "None of the candidate file formats ({}) match the "
                        "extensions of the primary ('{}') and auxiliary files"
                        "('{}') in {}"
                        .format(', '.join(str(c) for c in candidates),
                                self.path, self._potential_aux_files, self))
        return matches[0]

    def formatted(self, candidates):
        """
        Creates a copy of a fileset and sets the format of a fileset from a
        list of possible candidates.

        Parameters
        ----------
        candidates : list[FileFormat] | FileFormat
            A list of file-formats to select from.

        Returns
        -------
        formatted : Fileset
            A copy of the fileset with the format set
        """
        # Ensure candidates is a list of file formats (i.e. not a single frmat)
        if not isinstance(candidates, Iterable):
            candidates = [candidates]
        formatted = copy(self)
        format = formatted._format = self.select_format(candidates)  # @ReservedAssignment @IgnorePep8
        if format.aux_files and self._path is not None:
            formatted._aux_files = format.select_files(
                [self._path] + list(self._potential_aux_files))[1]
        # No longer need to retain potentials after we have assigned the real
        # auxiliaries
        formatted._potential_aux_files = None
        return formatted

    def matches_format(self, file_format):
        """
        Checks whether the provided file format matches the given fileset

        Parameters
        ----------
        file_format : FileFormat
            The file format to check.
        """
        try:
            self.select_format([file_format])
        except ArcanaFileFormatError:
            return False
        else:
            return True

    def dicom(self, index):
        """
        Returns a PyDicom object for the DICOM file at index 'index'

        Parameters
        ----------
        fileset : Fileset
            The fileset to read a DICOM file from
        index : int
            The index of the DICOM file in the fileset to read

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
        with open(op.join(self.path, fnames[index]), 'rb') as f:
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
                # Get the DICOM object for the first file in the fileset
                dcm = self.dicom(0)
                dct = {t: dcm[t].value for t in tags}
        except KeyError as e:
            e.msg = ("{} does not have dicom tag {}".format(
                     self, e.msg))
            raise e
        return dct

    def initkwargs(self):
        dct = BaseFileset.initkwargs(self)
        dct.update(BaseItem.initkwargs(self))
        dct['path'] = self.path
        dct['id'] = self.id
        dct['uri'] = self.uri
        dct['bids_attr'] = self.bids_attr
        dct['checksums'] = self.checksums
        dct['format_name'] = self._format_name
        dct['potential_aux_files'] = self._potential_aux_files
        return dct

    def get(self):
        if self.repository is not None:
            self._exists = True
            self._path, self._aux_files = self.repository.get_fileset(self)

    def put(self):
        if self.repository is not None and self._path is not None:
            self.repository.put_fileset(self)

    def contents_equal(self, other, **kwargs):
        """
        Test the equality of the fileset contents with another fileset. If the
        fileset's format implements a 'contents_equal' method than that is used
        to determine the equality, otherwise a straight comparison of the
        checksums is used.

        Parameters
        ----------
        other : Fileset
            The other fileset to compare to
        """
        if hasattr(self.format, 'contents_equal'):
            equal = self.format.contents_equal(self, other, **kwargs)
        else:
            equal = (self.checksums == other.checksums)
        return equal


class Field(BaseItem, BaseField):
    """
    A representation of a value field in the repository.

    Parameters
    ----------
    name : str
        The name of the fileset
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    derived : bool
        Whether or not the value belongs in the derived session or not
    subject_id : int | str | None
        The id of the subject which the field belongs to
    visit_id : int | str | None
        The id of the visit which the field belongs to
    repository : Repository
        The repository which the field is stored
    from_study : str
        Name of the Arcana study that that generated the field
    exists : bool
        Whether the field exists or is just a placeholder for a derivative
    record : arcana.pipeline.provenance.Record | None
        The provenance record for the pipeline that generated the field,
        if applicable
    """

    def __init__(self, name, value=None, dtype=None,
                 frequency='per_session', array=None, subject_id=None,
                 visit_id=None, repository=None, from_study=None,
                 exists=True, record=None):
        # Try to determine dtype and array from value if they haven't
        # been provided.
        if value is None:
            if dtype is None:
                raise ArcanaUsageError(
                    "Either 'value' or 'dtype' must be provided to "
                    "Field init")
            array = bool(array)  # Convert to array is None to False
        else:
            value = parse_value(value)
            if isinstance(value, list):
                if array is False:
                    raise ArcanaUsageError(
                        "Array value passed to '{}', which is explicitly not "
                        "an array ({})".format(name, value))
                array = True
            else:
                if array:
                    raise ArcanaUsageError(
                        "Non-array value ({}) passed to '{}', which expects "
                        "array{}".format(value, name,
                                         ('of type {}'.format(dtype)
                                          if dtype is not None else '')))
                array = False
            if dtype is None:
                if array:
                    dtype = type(value[0])
                else:
                    dtype = type(value)
            else:
                # Ensure everything is cast to the correct type
                if array:
                    value = [dtype(v) for v in value]
                else:
                    value = dtype(value)
        BaseField.__init__(self, name, dtype, frequency, array)
        BaseItem.__init__(self, subject_id, visit_id, repository,
                          from_study, exists, record)
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
        if self.array:
            val = '[' + ','.join(self._to_str(v) for v in self.value) + ']'
        else:
            val = self._to_str(self.value)
        return val

    def _to_str(self, val):
        if self.dtype is str:
            val = '"{}"'.format(val)
        else:
            val = str(val)
        return val

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
        return ("{}('{}',{} '{}', subj={}, vis={}, stdy={}, exists={})"
                .format(
                    type(self).__name__, self.name,
                    (" {},".format(self._value)
                     if self._value is not None else ''),
                    self.frequency, self.subject_id,
                    self.visit_id, self.from_study,
                    self.exists))

    @property
    def value(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name,
                "Cannot access value of {} as it hasn't been "
                "derived yet".format(repr(self)))
        if self._value is None:
            if self.repository is not None:
                self._value = self.repository.get_field(self)
            else:
                raise ArcanaError(
                    "Neither value nor repository has been set for Field("
                    "'{}')".format(self.name))
        return self._value

    @value.setter
    def value(self, value):
        if self.array:
            self._value = [self.dtype(v) for v in value]
        else:
            self._value = self.dtype(value)
        self._exists = True
        self.put()

    @property
    def checksums(self):
        """
        For duck-typing with filesets in checksum management. Instead of a
        checksum, just the value of the field is used
        """
        return self.value

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseItem.initkwargs(self))
        dct['value'] = self.value
        return dct

    def get(self):
        if self.repository is not None:
            self._exists = True
            self._value = self.repository.get_field(self)

    def put(self):
        if self.repository is not None and self._value is not None:
            self.repository.put_field(self)
