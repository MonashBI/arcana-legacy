import os.path as op
from arcana.exceptions import ArcanaUsageError
from .tree import Tree


class Dataset():
    """
    A representation of a "dataset", the complete collection of data
    (file-sets and fields) to be used in an analysis.

    Parameters
    ----------
    name : str
        The name/id/path that uniquely identifies the datset within the
        repository it is stored
    repository : Repository
        The repository the dataset belongs to
    subject_ids : list[str]
        Subject IDs to be included in the analysis. All other subjects are
        ignored
    visit_ids : list[str]
        Visit IDs to be included in the analysis. All other visits are ignored
    fill_tree : bool
        Whether to fill the tree of the destination repository with the
        provided subject and/or visit IDs. Intended to be used when the
        destination repository doesn't contain any of the the input
        filesets/fields (which are stored in external repositories) and
        so the sessions will need to be created in the destination
        repository.
    depth : int (0|1|2)
        The depth of the dataset (i.e. whether it has subjects and sessions).
            0 -> single session
            1 -> multiple subjects
            2 -> multiple subjects and visits
    subject_id_map : dict[str, str]
        Maps subject IDs in dataset to a global name-space
    visit_id_map : dict[str, str]
        Maps visit IDs in dataset to a global name-space
    """

    type = 'basic'

    def __init__(self, name, repository=None, subject_ids=None, visit_ids=None,
                 fill_tree=False, depth=0, subject_id_map=None,
                 visit_id_map=None, file_formats=()):
        if repository is None:
            # needs to be imported here to avoid circular imports
            from .local import LocalFileSystemRepo
            name = op.abspath(name)
            repository = LocalFileSystemRepo()
            if not op.exists(name):
                raise ArcanaUsageError(
                    "Base directory for LocalFileSystemRepo '{}' does not "
                    "exist".format(name))
        self._name = name
        self._repository = repository
        self._subject_ids = (tuple(subject_ids)
                             if subject_ids is not None else None)
        self._visit_ids = tuple(visit_ids) if visit_ids is not None else None
        self._fill_tree = fill_tree
        self._depth = depth

        self._subject_id_map = subject_id_map
        self._visit_id_map = visit_id_map
        self._inv_subject_id_map = {}
        self._inv_visit_id_map = {}
        self._file_formats = file_formats
        self.clear_cache()

    def __repr__(self):
        return "Dataset(name='{}', depth={}, repository={})".format(
            self.name, self.depth, self.repository)

    def __eq__(self, other):
        return (self.name == other.name
                and self.repository == other.repository
                and self._subject_ids == other._subject_ids
                and self._visit_ids == other._visit_ids
                and self._fill_tree == other._fill_tree
                and self.depth == other.depth)

    def __hash__(self):
        return (hash(self._name)
                ^ hash(self.repository)
                ^ hash(self._subject_ids)
                ^ hash(self._visit_ids)
                ^ hash(self._fill_tree)
                ^ hash(self._depth))

    @property
    def name(self):
        return self._name

    @property
    def repository(self):
        return self._repository

    @property
    def subject_ids(self):
        if self._subject_ids is None:
            return [s.id for s in self.tree.subjects]
        return self._subject_ids

    @property
    def visit_ids(self):
        if self._visit_ids is None:
            return [v.id for v in self.tree.visits]
        return self._visit_ids

    @property
    def prov(self):
        return {
            'name': self.name,
            'depth': self._depth,
            'repository': self.repository.prov,
            'subject_ids': tuple(self.subject_ids),
            'visit_ids': tuple(self.visit_ids)}

    @property
    def depth(self):
        return self._depth

    @property
    def num_subjects(self):
        return len(self.subject_ids)

    @property
    def num_visits(self):
        return len(self.visit_ids)

    @property
    def num_sessions(self):
        if self._visit_ids is None and self._subject_ids is None:
            num_sessions = len(list(self.tree.sessions))
        else:
            num_sessions = self.num_subjects * self.num_visits
        return num_sessions

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
        return self.repository.get_fileset(fileset)

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
        return self.repository.get_field(field)

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
        return self.repository.get_checksums(fileset)

    def put_fileset(self, fileset):
        """
        Inserts or updates the fileset into the repository

        Parameters
        ----------
        fileset : Fileset
            The fileset to insert into the repository
        """
        self.repository.put_fileset(fileset)

    def put_field(self, field):
        """
        Inserts or updates the fields into the repository

        Parameters
        ----------
        field : Field
            The field to insert into the repository
        """
        self.repository.put_field(field)

    def put_record(self, record):
        """
        Inserts a provenance record into a session or subject|visit|analysis
        summary

        Parameters
        ----------
        record : prov.Record
            The record to insert into the repository
        """
        self.repository.put_record(record, self)

    @property
    def tree(self):
        """
        Return the tree of subject and sessions information within a
        project in the XNAT repository

        Returns
        -------
        tree : arcana.repository.Tree
            A hierarchical tree of subject, session and fileset
            information for the repository
        """
        if self._cached_tree is None:
            # Find all data present in the repository (filtered by the passed
            # IDs)
            self._cached_tree = Tree.construct(
                self,
                *self.repository.find_data(
                    dataset=self,
                    subject_ids=self._subject_ids,
                    visit_ids=self._visit_ids),
                fill_subjects=(self._subject_ids if self._fill_tree else None),
                fill_visits=(self._visit_ids if self._fill_tree else None))
        return self._cached_tree

    def clear_cache(self):
        self._cached_tree = None

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

    def _map_id(self, id, map, inv_map):
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
