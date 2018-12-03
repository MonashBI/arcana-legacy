from builtins import object
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass
import logging
from collections import defaultdict
from itertools import chain
from .tree import Tree, Subject, Session, Visit


logger = logging.getLogger('arcana')


class BaseRepository(with_metaclass(ABCMeta, object)):
    """
    Abstract base class for all Repository systems, DaRIS, XNAT and
    local file system. Sets out the interface that all Repository
    classes should implement.
    """

    def __init__(self):
        self._connection_depth = 0
        self._cache = {}

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

    def __exit__(self, exception_type, exception_value, traceback):  # @UnusedVariable @IgnorePep8
        self._connection_depth -= 1
        if self._connection_depth == 0:
            self.disconnect()

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

    @abstractmethod
    def find_data(self, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all data within a repository, registering filesets, fields and
        provenance with the found_fileset, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
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

    def get_checksums(self, fileset):  # @UnusedVariable
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
        return None

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
    def put_record(self, record):
        """
        Inserts a provenance record into a session or subject|visit|study
        summary
        """

    def tree(self, subject_ids=None, visit_ids=None, **kwargs):
        """
        Return the tree of subject and sessions information within a
        project in the XNAT repository

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        tree : arcana.repository.Tree
            A hierarchical tree of subject, session and fileset
            information for the repository
        """
        # Find all data present in the repository (filtered by the passed IDs)
        return Tree.construct(*self.find_data(subject_ids=subject_ids,
                                              visit_ids=visit_ids),
                                              **kwargs)

    def cached_tree(self, subject_ids=None, visit_ids=None, fill=False):
        """
        Access the repository tree and caches it for subsequent
        accesses

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned
        fill : bool
            Create empty sessions for any that are missing in the
            subject_id x visit_id block. Typically only used if all
            the inputs to the study are coming from different repositories
            to the one that the derived products are stored in

        Returns
        -------
        tree : arcana.repository.Tree
            A hierarchical tree of subject, vist, session information and that
            of the filesets and fields they contain
        """
        if subject_ids is not None:
            subject_ids = frozenset(subject_ids)
        if visit_ids is not None:
            visit_ids = frozenset(visit_ids)
        try:
            tree = self._cache[(subject_ids, visit_ids)]
        except KeyError:
            if fill:
                fill_subjects = subject_ids
                fill_visits = visit_ids
            else:
                fill_subjects = fill_visits = None
            tree = self.tree(
                subject_ids=subject_ids, visit_ids=visit_ids,
                fill_visits=fill_visits, fill_subjects=fill_subjects)
            self._cache[(frozenset(tree.subject_ids),
                         frozenset(tree.visit_ids))] = tree
        return tree

    def clear_cache(self):
        self._cache = {}

    def __ne__(self, other):
        return not (self == other)
