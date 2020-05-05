from abc import ABCMeta, abstractmethod
import logging
from .dataset import Dataset


logger = logging.getLogger('arcana')


class Repository(metaclass=ABCMeta):
    """
    Abstract base class for all Repository systems, DaRIS, XNAT and
    local file system. Sets out the interface that all Repository
    classes should implement.
    """

    def __init__(self):
        self._connection_depth = 0

    def __enter__(self):
        # This allows the repository to be used within nested contexts
        # but still only use one connection. This is useful for calling
        # methods that need connections, and therefore control their
        # own connection, in batches using the same connection by
        # placing the batch calls within an outer context.
        if self._connection_depth == 0:
            self.connect()
        self._connection_depth += 1
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._connection_depth -= 1
        if self._connection_depth == 0:
            self.disconnect()

    def standardise_name(self, name):
        return name

    def connect(self):
        """
        If a connection session is required to the repository,
        manage it here
        """

    def disconnect(self):
        """
        If a connection session is required to the repository,
        manage it here
        """

    def dataset(self, name, **kwargs):
        """
        Returns a dataset from the XNAT repository

        Parameters
        ----------
        name : str
            The name, path or ID of the dataset within the repository
        subject_ids : list[str]
            The list of subjects to include in the dataset
        visit_ids : list[str]
            The list of visits to include in the dataset
        """
        return Dataset(name, repository=self, **kwargs)

    @abstractmethod
    def find_data(self, dataset, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all data within a repository, registering filesets, fields and
        provenance with the found_fileset, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        dataset : Dataset
            The dataset to return the data for
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        filesets : list[Fileset]
            All the filesets found in the repository
        fields : list[Field]
            All the fields found in the repository
        records : list[Record]
            The provenance records found in the repository
        """

    @abstractmethod
    def get_fileset(self, fileset):
        """
        Cache the fileset locally if required

        Parameters
        ----------
        fileset : Fileset
            The fileset to cache locally

        Returns
        -------
        path : str
            The file-system path to the cached file
        """

    @abstractmethod
    def get_field(self, field):
        """
        Extract the value of the field from the repository

        Parameters
        ----------
        field : Field
            The field to retrieve the value for

        Returns
        -------
        value : int | float | str | list[int] | list[float] | list[str]
            The value of the Field
        """

    def get_checksums(self, fileset):
        """
        Returns the checksums for the files in the fileset that are stored in
        the repository. If no checksums are stored in the repository then this
        method should be left to return None and the checksums will be
        calculated by downloading the files and taking calculating the digests

        Parameters
        ----------
        fileset : Fileset
            The fileset to return the checksums for

        Returns
        -------
        checksums : dct[str, str]
            A dictionary with keys corresponding to the relative paths of all
            files in the fileset from the base path and values equal to the MD5
            hex digest. The primary file in the file-set (i.e. the one that the
            path points to) should be specified by '.'.
        """

    @abstractmethod
    def put_fileset(self, fileset):
        """
        Inserts or updates the fileset into the repository

        Parameters
        ----------
        fileset : Fileset
            The fileset to insert into the repository
        """

    @abstractmethod
    def put_field(self, field):
        """
        Inserts or updates the fields into the repository

        Parameters
        ----------
        field : Field
            The field to insert into the repository
        """

    @abstractmethod
    def put_record(self, record, dataset):
        """
        Inserts a provenance record into a session or subject|visit|analysis
        summary

        Parameters
        ----------
        record : prov.Record
            The record to insert into the repository
        dataset : Dataset
            The dataset to put the record into
        """
