from builtins import zip
from builtins import object
from itertools import chain, groupby
from collections import defaultdict
from operator import attrgetter, itemgetter
from collections import OrderedDict
import logging
from arcana.data import BaseFileset, BaseField
from arcana.utils import split_extension
from arcana.exceptions import (
    ArcanaNameError, ArcanaRepositoryError)

id_getter = attrgetter('id')

logger = logging.getLogger('arcana')


class TreeNode(object):

    def __init__(self, filesets, fields, records):
        if filesets is None:
            filesets = []
        if fields is None:
            fields = []
        if records is None:
            records = []
        # Save filesets and fields in ordered dictionary by name and
        # name of study that generated them (if applicable)
        self._filesets = OrderedDict()
        for fileset in sorted(filesets):
            id_key = (fileset.id, fileset.from_study)
            try:
                dct = self._filesets[id_key]
            except KeyError:
                dct = self._filesets[id_key] = OrderedDict()
            if fileset.format_name is not None:
                format_key = fileset.format_name
            else:
                format_key = split_extension(fileset.path)[1]
            if format_key in dct:
                raise ArcanaRepositoryError(
                    "Attempting to add duplicate filesets to tree ({} and {})"
                    .format(fileset, dct[format_key]))
            dct[format_key] = fileset
        self._fields = OrderedDict(((f.name, f.from_study), f)
                                   for f in sorted(fields))
        self._records = OrderedDict(
            ((r.pipeline_name, r.from_study), r)
            for r in sorted(records, key=lambda r: (r.subject_id, r.visit_id,
                                                    r.from_study)))
        self._missing_records = []
        self._duplicate_records = []
        # Match up provenance records with items in the node
        for item in chain(self.filesets, self.fields):
            if not item.derived:
                continue  # Skip acquired items
            records = [r for r in self.records
                       if (item.from_study == r.from_study and
                           item.name in r.outputs)]
            if not records:
                self._missing_records.append(item.name)
            elif len(records) > 1:
                item.record = sorted(records, key=attrgetter('datetime'))[-1]
                self._duplicate_records.append(item.name)
            else:
                item.record = records[0]

    def __eq__(self, other):
        if not (isinstance(other, type(self)) or
                isinstance(self, type(other))):
            return False
        return (tuple(self.filesets) == tuple(other.filesets) and
                tuple(self.fields) == tuple(other.fields) and
                tuple(self.records) == tuple(other.records))

    def __hash__(self):
        return (hash(tuple(self.filesets)) ^ hash(tuple(self.fields)) ^
                hash(tuple(self.fields)))

    @property
    def filesets(self):
        return chain(*(d.values() for d in self._filesets.values()))

    @property
    def fields(self):
        return self._fields.values()

    @property
    def records(self):
        return self._records.values()

    @property
    def subject_id(self):
        "To be overridden by subclasses where appropriate"
        return None

    @property
    def visit_id(self):
        "To be overridden by subclasses where appropriate"
        return None

    def fileset(self, id, from_study=None, format=None):  # @ReservedAssignment @IgnorePep8
        """
        Gets the fileset with the ID 'id' produced by the Study named 'study'
        if provided. If a spec is passed instead of a str to the name argument,
        then the study will be set from the spec iff it is derived

        Parameters
        ----------
        id : str | FilesetSpec
            The name of the fileset or a spec matching the given name
        from_study : str | None
            Name of the study that produced the fileset if derived. If None
            and a spec is passed instaed of string to the name argument then
            the study name will be taken from the spec instead.
        format : FileFormat | str | None
            Either the format of the fileset to return or the name of the
            format. If None and only a single fileset is found for the given
            name and study then that is returned otherwise an exception is
            raised
        """
        if isinstance(id, BaseFileset):
            if from_study is None and id.derived:
                from_study = id.study.name
            id = id.name  # @ReservedAssignment
        try:
            format_dct = self._filesets[(id, from_study)]
        except KeyError:
            available = [
                ('{}(format={})'.format(f.id, f._resource_name)
                 if f._resource_name is not None else f.id)
                for f in self.filesets if f.from_study == from_study]
            other_studies = [
                (f.from_study if f.from_study is not None else '<root>')
                for f in self.filesets if f.id == id]
            if other_studies:
                msg = (". NB: matching fileset(s) found for '{}' study(ies) "
                       "('{}')".format(id, "', '".join(other_studies)))
            else:
                msg = ''
            raise ArcanaNameError(
                id,
                ("{} doesn't have a fileset named '{}'{} "
                   "(available '{}'){}"
                   .format(self, id,
                           (" from study '{}'".format(from_study)
                            if from_study is not None else ''),
                           "', '".join(available), msg)))
        else:
            if format is None:
                all_formats = list(format_dct.values())
                if len(all_formats) > 1:
                    raise ArcanaNameError(
                        "Multiple filesets found for '{}'{} in {} with formats"
                        " {}. Need to specify a format"
                        .format(id, ("in '{}'".format(from_study)
                                       if from_study is not None else ''),
                                self, "', '".join(format_dct.keys())))
                fileset = all_formats[0]
            else:
                try:
                    if isinstance(format, str):
                        fileset = format_dct[format]
                    else:
                        try:
                            fileset = format_dct[format.ext]
                        except KeyError:
                            fileset = None
                            for rname, rfileset in format_dct.items():
                                if rname in format.resource_names(
                                        self.tree.repository.type):
                                    fileset = rfileset
                                    break
                            if fileset is None:
                                raise
                except KeyError:
                    raise ArcanaNameError(
                        format,
                        ("{} doesn't have a fileset named '{}'{} with "
                         "format '{}' (available '{}'){}"
                           .format(self, id,
                                   (" from study '{}'".format(from_study)
                                    if from_study is not None else ''),
                                   format,
                                   "', '".join(format_dct.keys()), msg)))

        return fileset

    def field(self, name, from_study=None):
        """
        Gets the field named 'name' produced by the Study named 'study' if
        provided. If a spec is passed instead of a str to the name argument,
        then the study will be set from the spec iff it is derived

        Parameters
        ----------
        name : str | BaseField
            The name of the field or a spec matching the given name
        study : str | None
            Name of the study that produced the field if derived. If None
            and a spec is passed instaed of string to the name argument then
            the study name will be taken from the spec instead.
        """
        if isinstance(name, BaseField):
            if from_study is None and name.derived:
                from_study = name.study.name
            name = name.name
        try:
            return self._fields[(name, from_study)]
        except KeyError:
            available = [d.name for d in self.fields
                         if d.from_study == from_study]
            other_studies = [(d.from_study if d.from_study is not None
                              else '<root>')
                             for d in self.fields
                             if d.name == name]
            if other_studies:
                msg = (". NB: matching field(s) found for '{}' study(ies) "
                       "('{}')".format(name, "', '".join(other_studies)))
            else:
                msg = ''
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}'{} "
                       "(available '{}')"
                       .format(
                           self, name,
                           (" from study '{}'".format(from_study)
                            if from_study is not None else ''),
                           "', '".join(available), msg)))

    def record(self, pipeline_name, from_study):
        """
        Returns the provenance record for a given pipeline

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline that generated the record
        from_study : str
            The name of the study that the pipeline was generated from

        Returns
        -------
        record : arcana.provenance.Record
            The provenance record generated by the specified pipeline
        """
        try:
            return self._records[(pipeline_name, from_study)]
        except KeyError:
            found = []
            for sname, pnames in groupby(sorted(self._records,
                                                key=itemgetter(1)),
                                         key=itemgetter(1)):
                found.append(
                    "'{}' for '{}'".format("', '".join(p for p, _ in pnames),
                                           sname))
            raise ArcanaNameError(
                (pipeline_name, from_study),
                ("{} doesn't have a provenance record for pipeline '{}' "
                 "for '{}' study (found {})".format(
                     self, pipeline_name, from_study,
                     '; '.join(found))))

    @property
    def data(self):
        return chain(self.filesets, self.fields)

    def __ne__(self, other):
        return not (self == other)

    def find_mismatch(self, other, indent=''):
        """
        Highlights where two nodes differ in a human-readable form

        Parameters
        ----------
        other : TreeNode
            The node to compare
        indent : str
            The white-space with which to indent output string

        Returns
        -------
        mismatch : str
            The human-readable mismatch string
        """
        if self != other:
            mismatch = "\n{}{}".format(indent, type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if len(list(self.filesets)) != len(list(other.filesets)):
            mismatch += ('\n{indent}mismatching summary fileset lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.filesets)),
                                 len(list(other.filesets)),
                                 list(self.filesets),
                                 list(other.filesets),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.filesets, other.filesets):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        if len(list(self.fields)) != len(list(other.fields)):
            mismatch += ('\n{indent}mismatching summary field lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.fields)),
                                 len(list(other.fields)),
                                 list(self.fields),
                                 list(other.fields),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.fields, other.fields):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch


class Tree(TreeNode):
    """
    Represents a project tree as stored in a repository

    Parameters
    ----------
    subjects : List[Subject]
        List of subjects
    visits : List[Visits]
        List of visits in the project across subjects
        (i.e. timepoint 1, 2, 3)
    repository : Repository
        The repository that the tree comes from
    filesets : List[Fileset]
        The filesets that belong to the project, i.e. of 'per_study'
        frequency
    fields : List[Field]
        The fields that belong to the project, i.e. of 'per_study'
        frequency
    fill_subjects : list[int] | None
        Create empty sessions for any subjects that are missing
        from the provided list. Typically only used if all
        the inputs to the study are coming from different repositories
        to the one that the derived products are stored in
    fill_visits : list[int] | None
        Create empty sessions for any visits that are missing
        from the provided list. Typically only used if all
        the inputs to the study are coming from different repositories
        to the one that the derived products are stored in
    """

    frequency = 'per_study'

    def __init__(self, subjects, visits, repository, filesets=None,
                 fields=None, records=None, fill_subjects=None,
                 fill_visits=None, **kwargs):  # @UnusedVariable @IgnorePep8
        TreeNode.__init__(self, filesets, fields, records)
        self._subjects = OrderedDict(sorted(
            ((s.id, s) for s in subjects), key=itemgetter(0)))
        self._visits = OrderedDict(sorted(
            ((v.id, v) for v in visits), key=itemgetter(0)))
        if fill_subjects is not None or fill_visits is not None:
            self._fill_empty_sessions(fill_subjects, fill_visits)
        for subject in self.subjects:
            subject.tree = self
        for visit in self.visits:
            visit.tree = self
        for session in self.sessions:
            session.tree = self
        self._repository = repository
        # Collate missing and duplicates provenance records for single warnings
        missing_records = defaultdict(lambda: defaultdict(list))
        duplicate_records = defaultdict(lambda: defaultdict(list))
        for node in self.nodes():
            for missing in node._missing_records:
                missing_records[missing][node.visit_id].append(node.subject_id)
            for duplicate in node._duplicate_records:
                duplicate_records[duplicate][node.visit_id].append(
                    node.subject_id)
        for name, ids in missing_records.items():
            logger.warning(
                "No provenance records found for {} derivative in "
                "the following nodes: {}. Will assume they are a "
                "\"protected\" (manually created) derivatives"
                .format(name, '; '.join("visit='{}', subjects={}".format(k, v)
                                        for k, v in ids.items())))
        for name, ids in duplicate_records.items():
            logger.warning(
                "Duplicate provenance records found for {} in the following "
                "nodes: {}. Will select the latest record in each case"
                .format(name, '; '.join("visit='{}', subjects={}".format(k, v)
                                        for k, v in ids.items())))

    def __eq__(self, other):
        return (super(Tree, self).__eq__(other) and
                self._subjects == other._subjects and
                self._visits == other._visits)

    def __hash__(self):
        return (TreeNode.__hash_(self) ^
                hash(tuple(self.subjects)) ^
                hash(tuple(self._visits)))

    @property
    def repository(self):
        return self._repository

    @property
    def subjects(self):
        return self._subjects.values()

    @property
    def visits(self):
        return self._visits.values()

    @property
    def sessions(self):
        return chain(*(s.sessions for s in self.subjects))

    @property
    def tree(self):
        return self

    @property
    def subject_ids(self):
        return self._subjects.keys()

    @property
    def visit_ids(self):
        return self._visits.keys()

    @property
    def session_ids(self):
        return ((s.subject_id, s.visit_id) for s in self.sessions)

    @property
    def complete_subjects(self):
        max_num_sessions = max(len(s) for s in self.subjects)
        return (s for s in self.subjects if len(s) == max_num_sessions)

    @property
    def complete_visits(self):
        max_num_sessions = max(len(v) for v in self.visits)
        return (v for v in self.visits if len(v) == max_num_sessions)

    @property
    def incomplete_subjects(self):
        max_num_sessions = max(len(s) for s in self.subjects)
        return (s for s in self.subjects if len(s) != max_num_sessions)

    @property
    def incomplete_visits(self):
        max_num_sessions = max(len(v) for v in self.visits)
        return (v for v in self.visits if len(v) != max_num_sessions)

    def subject(self, id):  # @ReservedAssignment
        try:
            return self._subjects[str(id)]
        except KeyError:
            raise ArcanaNameError(
                id, ("{} doesn't have a subject named '{}' ('{}')"
                       .format(self, id, "', '".join(self._subjects))))

    def visit(self, id):  # @ReservedAssignment
        try:
            return self._visits[str(id)]
        except KeyError:
            raise ArcanaNameError(
                id, ("{} doesn't have a visit named '{}' ('{}')"
                       .format(self, id, "', '".join(self._visits))))

    def session(self, subject_id, visit_id):
        return self.subject(subject_id).session(visit_id)

    def __iter__(self):
        return self.nodes()

    def nodes(self, frequency=None):
        """
        Returns an iterator over all nodes in the tree for the specified
        frequency. If no frequency is specified then all nodes are returned

        Parameters
        ----------
        frequency : str | None
            The frequency of the nodes to iterate over. If None all
            frequencies are returned

        Returns
        -------
        nodes : iterable[TreeNode]
        """
        if frequency is None:
            nodes = chain(*(self._nodes(f)
                            for f in ('per_study', 'per_subject',
                                      'per_visit', 'per_session')))
        else:
            nodes = self._nodes(frequency=frequency)
        return nodes

    def _nodes(self, frequency):
        if frequency == 'per_session':
            nodes = chain(*(s.sessions for s in self.subjects))
        elif frequency == 'per_subject':
            nodes = self.subjects
        elif frequency == 'per_visit':
            nodes = self.visits
        elif frequency == 'per_study':
            nodes = [self]
        else:
            assert False
        return nodes

    def find_mismatch(self, other, indent=''):
        """
        Used in debugging unittests
        """
        mismatch = super(Tree, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if len(list(self.subjects)) != len(list(other.subjects)):
            mismatch += ('\n{indent}mismatching subject lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.subjects)),
                                 len(list(other.subjects)),
                                 list(self.subjects),
                                 list(other.subjects),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.subjects, other.subjects):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        if len(list(self.visits)) != len(list(other.visits)):
            mismatch += ('\n{indent}mismatching visit lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.visits)),
                                 len(list(other.visits)),
                                 list(self.visits),
                                 list(other.visits),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.visits, other.visits):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __repr__(self):
        return ("Tree(num_subjects={}, num_visits={}, "
                "num_filesets={}, num_fields={})".format(
                    len(list(self.subjects)),
                    len(list(self.visits)),
                    len(list(self.filesets)), len(list(self.fields))))

    def _fill_empty_sessions(self, fill_subjects, fill_visits):
        """
        Fill in tree with additional empty subjects and/or visits to
        allow the study to pull its inputs from external repositories
        """
        if fill_subjects is None:
            fill_subjects = [s.id for s in self.subjects]
        if fill_visits is None:
            fill_visits = [v.id for v in self.complete_visits]
        for subject_id in fill_subjects:
            try:
                subject = self.subject(subject_id)
            except ArcanaNameError:
                subject = self._subjects[subject_id] = Subject(
                    subject_id, [], [], [])
            for visit_id in fill_visits:
                try:
                    subject.session(visit_id)
                except ArcanaNameError:
                    session = Session(subject_id, visit_id, [], [])
                    subject._sessions[visit_id] = session
                    try:
                        visit = self.visit(visit_id)
                    except ArcanaNameError:
                        visit = self._visits[visit_id] = Visit(
                            visit_id, [], [], [])
                    visit._sessions[subject_id] = session

    @classmethod
    def construct(cls, repository, filesets=(), fields=(), records=(),
                  file_formats=(), **kwargs):
        """
        Return the hierarchical tree of the filesets and fields stored in a
        repository

        Parameters
        ----------
        respository : Repository
            The repository that the tree comes from
        filesets : list[Fileset]
            List of all filesets in the tree
        fields : list[Field]
            List of all fields in the tree
        records : list[Record]
            List of all records in the tree

        Returns
        -------
        tree : arcana.repository.Tree
            A hierarchical tree of subject, session and fileset
            information for the repository
        """
        # Sort the data by subject and visit ID
        filesets_dict = defaultdict(list)
        for fset in filesets:
            if file_formats:
                fset.set_format(file_formats)
            filesets_dict[(fset.subject_id, fset.visit_id)].append(fset)
        fields_dict = defaultdict(list)
        for field in fields:
            fields_dict[(field.subject_id, field.visit_id)].append(field)
        records_dict = defaultdict(list)
        for record in records:
            records_dict[(record.subject_id, record.visit_id)].append(record)
        # Create all sessions
        subj_sessions = defaultdict(list)
        visit_sessions = defaultdict(list)
        for sess_id in set(chain(filesets_dict, fields_dict,
                                 records_dict)):
            if None in sess_id:
                continue  # Save summaries for later
            subj_id, visit_id = sess_id
            session = Session(
                subject_id=subj_id, visit_id=visit_id,
                filesets=filesets_dict[sess_id],
                fields=fields_dict[sess_id],
                records=records_dict[sess_id])
            subj_sessions[subj_id].append(session)
            visit_sessions[visit_id].append(session)
        subjects = []
        for subj_id in subj_sessions:
            subjects.append(Subject(
                subj_id,
                sorted(subj_sessions[subj_id]),
                filesets_dict[(subj_id, None)],
                fields_dict[(subj_id, None)],
                records_dict[(subj_id, None)]))
        visits = []
        for visit_id in visit_sessions:
            visits.append(Visit(
                visit_id,
                sorted(visit_sessions[visit_id]),
                filesets_dict[(None, visit_id)],
                fields_dict[(None, visit_id)],
                records_dict[(None, visit_id)]))
        return Tree(sorted(subjects),
                    sorted(visits),
                    repository,
                    filesets_dict[(None, None)],
                    fields_dict[(None, None)],
                    records_dict[(None, None)],
                    **kwargs)


class Subject(TreeNode):
    """
    Represents a subject as stored in a repository

    Parameters
    ----------
    subject_id : str
        The ID of the subject
    sessions : List[Session]
        The sessions in the subject
    filesets : List[Fileset]
        The filesets that belong to the subject, i.e. of 'per_subject'
        frequency
    fields : List[Field]
        The fields that belong to the subject, i.e. of 'per_subject'
        frequency
    """

    frequency = 'per_subject'

    def __init__(self, subject_id, sessions, filesets=None,
                 fields=None, records=None):
        TreeNode.__init__(self, filesets, fields, records)
        self._id = subject_id
        self._sessions = OrderedDict(sorted(
            ((s.visit_id, s) for s in sessions), key=itemgetter(0)))
        for session in self.sessions:
            session.subject = self
        self._tree = None

    @property
    def id(self):
        return self._id

    @property
    def subject_id(self):
        return self.id

    @property
    def tree(self):
        return self._tree

    @tree.setter
    def tree(self, tree):
        self._tree = tree

    def __lt__(self, other):
        return self._id < other._id

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other)and
                self._id == other._id and
                self._sessions == other._sessions)

    def __hash__(self):
        return (TreeNode.__hash__(self) ^
                hash(self._id) ^
                hash(tuple(self.sessions)))

    def __len__(self):
        return len(self._sessions)

    def __iter__(self):
        return self.sessions

    @property
    def sessions(self):
        return self._sessions.values()

    def nodes(self, frequency=None):
        """
        Returns all sessions in the subject. If a frequency is passed then
        it will return all nodes of that frequency related to the current node.
        If there is no relationshop between the current node and the frequency
        then all nodes in the tree for that frequency will be returned

        Parameters
        ----------
        frequency : str | None
            The frequency of the nodes to return

        Returns
        -------
        nodes : iterable[TreeNode]
            All nodes related to the subject for the specified frequency, or
            all nodes in the tree if there is no relation with that frequency
            (e.g. per_visit)
        """
        if frequency in (None, 'per_session'):
            return self.sessions
        elif frequency == 'per_visit':
            return self.parent.nodes(frequency)
        elif frequency == 'per_subject':
            return [self]
        elif frequency == 'per_study':
            return [self.parent]

    @property
    def visit_ids(self):
        return self._sessions.values()

    def session(self, visit_id):
        try:
            return self._sessions[str(visit_id)]
        except KeyError:
            raise ArcanaNameError(
                visit_id, ("{} doesn't have a session named '{}' ('{}')"
                           .format(self, visit_id,
                                   "', '".join(self._sessions))))

    def find_mismatch(self, other, indent=''):
        mismatch = TreeNode.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.id != other.id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self.id, other.id))
        if len(list(self.sessions)) != len(list(other.sessions)):
            mismatch += ('\n{indent}mismatching session lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.sessions)),
                                 len(list(other.sessions)),
                                 list(self.sessions),
                                 list(other.sessions),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.sessions, other.sessions):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Subject(id={}, num_sessions={})"
                .format(self._id, len(self._sessions)))


class Visit(TreeNode):
    """
    Represents a collection of visits across subjects (e.g. time-point 1)
    as stored in a repository

    Parameters
    ----------
    visit_id : str
        The ID of the visit
    sessions : List[Session]
        The sessions in the visit
    filesets : List[Fileset]
        The filesets that belong to the visit, i.e. of 'per_visit'
        frequency
    fields : List[Field]
        The fields that belong to the visit, i.e. of 'per_visit'
        frequency
    """

    frequency = 'per_visit'

    def __init__(self, visit_id, sessions, filesets=None, fields=None,
                 records=None):
        TreeNode.__init__(self, filesets, fields, records)
        self._id = visit_id
        self._sessions = OrderedDict(sorted(
            ((s.subject_id, s) for s in sessions), key=itemgetter(0)))
        for session in sessions:
            session.visit = self
        self._tree = None

    @property
    def id(self):
        return self._id

    @property
    def visit_id(self):
        return self.id

    @property
    def tree(self):
        return self._tree

    @tree.setter
    def tree(self, tree):
        self._tree = tree

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other) and
                self._id == other._id and
                self._sessions == other._sessions)

    def __hash__(self):
        return (TreeNode.__hash__(self) ^
                hash(self._id) ^
                hash(tuple(self.sessions)))

    def __lt__(self, other):
        return self._id < other._id

    def __len__(self):
        return len(self._sessions)

    def __iter__(self):
        return self.sessions

    @property
    def sessions(self):
        return self._sessions.values()

    def nodes(self, frequency=None):
        """
        Returns all sessions in the visit. If a frequency is passed then
        it will return all nodes of that frequency related to the current node.
        If there is no relationshop between the current node and the frequency
        then all nodes in the tree for that frequency will be returned

        Parameters
        ----------
        frequency : str | None
            The frequency of the nodes to return

        Returns
        -------
        nodes : iterable[TreeNode]
            All nodes related to the visit for the specified frequency, or
            all nodes in the tree if there is no relation with that frequency
            (e.g. per_subject)
        """
        if frequency in (None, 'per_session'):
            return self.sessions
        elif frequency == 'per_subject':
            return self.parent.nodes(frequency)
        elif frequency == 'per_visit':
            return [self]
        elif frequency == 'per_study':
            return [self.parent]

    def session(self, subject_id):
        try:
            return self._sessions[str(subject_id)]
        except KeyError:
            raise ArcanaNameError(
                subject_id, ("{} doesn't have a session named '{}' ('{}')"
                             .format(self, subject_id,
                                     "', '".join(self._sessions))))

    def find_mismatch(self, other, indent=''):
        mismatch = TreeNode.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.id != other.id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self.id, other.id))
        if len(list(self.sessions)) != len(list(other.sessions)):
            mismatch += ('\n{indent}mismatching session lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.sessions)),
                                 len(list(other.sessions)),
                                 list(self.sessions),
                                 list(other.sessions),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.sessions, other.sessions):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Visit(id={}, num_sessions={})".format(self._id,
                                                      len(self._sessions))


class Session(TreeNode):
    """
    Represents a session stored in a repository

    Parameters
    ----------
    subject_id : str
        The subject ID of the session
    visit_id : str
        The visit ID of the session
    filesets : list(Fileset)
        The filesets found in the session
    derived : dict[str, Session]
        Sessions storing derived scans are stored for separate analyses
    """

    frequency = 'per_session'

    def __init__(self, subject_id, visit_id, filesets=None, fields=None,
                 records=None):
        TreeNode.__init__(self, filesets, fields, records)
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._subject = None
        self._visit = None
        self._tree = None

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def subject_id(self):
        return self._subject_id

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other) and
                self.subject_id == other.subject_id and
                self.visit_id == other.visit_id)

    def __hash__(self):
        return (TreeNode.__hash__(self) ^
                hash(self.subject_id) ^
                hash(self.visit_id))

    def __lt__(self, other):
        if self.subject_id < other.subject_id:
            return True
        else:
            return self.visit_id < other.visit_id

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        self._subject = subject

    @property
    def visit(self):
        return self._visit

    @visit.setter
    def visit(self, visit):
        self._visit = visit

    @property
    def tree(self):
        return self._tree

    @tree.setter
    def tree(self, tree):
        self._tree = tree

    def nodes(self, frequency=None):
        """
        Returns all nodes of the specified frequency that are related to
        the given Session

        Parameters
        ----------
        frequency : str | None
            The frequency of the nodes to return

        Returns
        -------
        nodes : iterable[TreeNode]
            All nodes related to the Session for the specified frequency
        """
        if frequency is None:
            []
        elif frequency == 'per_session':
            return [self]
        elif frequency in ('per_visit', 'per_subject'):
            return [self.parent]
        elif frequency == 'per_study':
            return [self.parent.parent]

    def find_mismatch(self, other, indent=''):
        mismatch = TreeNode.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.subject_id != other.subject_id:
            mismatch += ('\n{}subject_id: self={} v other={}'
                         .format(sub_indent, self.subject_id,
                                 other.subject_id))
        if self.visit_id != other.visit_id:
            mismatch += ('\n{}visit_id: self={} v other={}'
                         .format(sub_indent, self.visit_id,
                                 other.visit_id))
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Session(subject_id='{}', visit_id='{}', num_filesets={}, "
                "num_fields={})".format(
                    self.subject_id, self.visit_id, len(self._filesets),
                    len(self._fields)))
