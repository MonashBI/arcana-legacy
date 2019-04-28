from builtins import object
from collections import defaultdict
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass
import logging
from .tree import Tree
from arcana.exceptions import ArcanaUsageError


logger = logging.getLogger('arcana')


class Repository(with_metaclass(ABCMeta, object)):
    """
    Abstract base class for all Repository systems, DaRIS, XNAT and
    local file system. Sets out the interface that all Repository
    classes should implement.
    """

    def __init__(self, subject_id_map=None, visit_id_map=None,
                 file_formats=()):
        self._connection_depth = 0
        self._subject_id_map = subject_id_map
        self._visit_id_map = visit_id_map
        self._inv_subject_id_map = {}
        self._inv_visit_id_map = {}
        self._file_formats = file_formats
        self.clear_cache()

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
        return Tree.construct(
            self, *self.find_data(subject_ids=subject_ids,
                                  visit_ids=visit_ids), **kwargs)

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
            tree = self._cache[subject_ids][visit_ids]
        except KeyError:
            if fill:
                fill_subjects = subject_ids
                fill_visits = visit_ids
            else:
                fill_subjects = fill_visits = None
            tree = self.tree(
                subject_ids=subject_ids, visit_ids=visit_ids,
                fill_visits=fill_visits, fill_subjects=fill_subjects)
            # Save the tree within the cache under the given subject/
            # visit ID filters and the IDs that were actually returned
            self._cache[subject_ids][visit_ids] = self._cache[
                frozenset(tree.subject_ids)][frozenset(tree.visit_ids)] = tree
        return tree

    def clear_cache(self):
        self._cache = defaultdict(dict)

    def __ne__(self, other):
        return not (self == other)

    def map_subject_id(self, subject_id):
        return self._map_id(subject_id, self._subject_id_map,
                            self._inv_subject_id_map)

    def map_visit_id(self, visit_id):
        return self._map_id(visit_id, self._visit_id_map,
                            self._inv_visit_id_map)

    def inv_map_subject_id(self, subject_id):
        try:
            return self._inv_subject_id_map[subject_id]
        except KeyError:
            return subject_id

    def inv_map_visit_id(self, visit_id):
        try:
            return self._inv_visit_id_map[visit_id]
        except KeyError:
            return visit_id

    def _map_id(self, id, map, inv_map):  # @ReservedAssignment
        if id is None:
            return None
        mapped = id
        if callable(map):
            mapped = map(id)
        elif self._visit_id_map is not None:
            try:
                mapped = map(id)
            except KeyError:
                pass
        if mapped != id:
            # Check for multiple mappings onto the same ID
            try:
                prev = inv_map[mapped]
            except KeyError:
                inv_map[mapped] = id
            else:
                if prev != id:
                    raise ArcanaUsageError(
                        "Both '{}' and '{}' have been mapped onto the same ID "
                        "in repository {}"
                        .format(prev, id, self))
        return mapped
