from builtins import object
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass
import logging

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
    def get_fileset(self, fileset):
        """
        If the repository is remote, cache the fileset here
        """
        pass

    @abstractmethod
    def get_field(self, field):
        """
        If the repository is remote, cache the fileset here
        """
        pass

    @abstractmethod
    def put_fileset(self, fileset):
        """
        Inserts or updates the fileset into the repository
        """

    @abstractmethod
    def put_field(self, field):
        """
        Inserts or updates the fields into the repository
        """

    @abstractmethod
    def put_provenance(self, record):
        """
        Inserts a provenance record into a session or subject|visit|study
        summary
        """

    @abstractmethod
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

    def cached_tree(self, subject_ids=None, visit_ids=None,
                    fill=False):
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
        """
        if subject_ids is not None:
            subject_ids = frozenset(subject_ids)
        if visit_ids is not None:
            visit_ids = frozenset(visit_ids)
        try:
            tree = self._cache[(subject_ids, visit_ids, fill)]
        except KeyError:
            fill_subjects = None
            fill_visits = None
            if fill:
                if subject_ids is not None:
                    fill_subjects = subject_ids
                if visit_ids is not None:
                    fill_visits = visit_ids
            tree = self._cache[(subject_ids, visit_ids, fill)] = self.tree(
                subject_ids=subject_ids, visit_ids=visit_ids,
                fill_visits=fill_visits, fill_subjects=fill_subjects)
        return tree

    def clear_cache(self):
        self._cache = {}

    def __ne__(self, other):
        return not (self == other)
