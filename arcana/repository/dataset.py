import os.path as op
from arcana.exceptions import ArcanaError
from .local import LocalFileSystemRepo


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
    """

    def __init__(self, name, repository=None, subject_ids=None, visit_ids=None,
                 fill_tree=False, depth=2, **kwargs):
        if repository is None:
            name = op.abspath(name)
            repository = LocalFileSystemRepo(**kwargs)
            if not op.exists(name):
                raise ArcanaError(
                    "Base directory for LocalFileSystemRepo '{}' does not "
                    "exist".format(name))
        self._name = name
        self._repository = repository
        self._subject_ids = (tuple(subject_ids)
                             if subject_ids is not None else None)
        self._visit_ids = tuple(visit_ids) if visit_ids is not None else None
        self._fill_tree = fill_tree
        self._depth = depth

    def __repr__(self):
        return "Dataset(name='{}', repository={})".format(self.name,
                                                          self.repository)

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
    def tree(self):
        return self._repository.cached_tree(
            subject_ids=self._subject_ids,
            visit_ids=self._visit_ids,
            fill=self._fill_tree)

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
