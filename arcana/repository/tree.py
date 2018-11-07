from builtins import zip
from builtins import object
from itertools import chain
from operator import attrgetter, itemgetter
from collections import OrderedDict
from arcana.exception import (
    ArcanaNameError, ArcanaMissingDataException,
    ArcanaProvenanceRecordMismatchError, ArcanaError)

id_getter = attrgetter('id')


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
        self._filesets = OrderedDict(((d.name, d.from_study), d)
                                     for d in filesets)
        self._fields = OrderedDict(((f.name, f.from_study), f)
                                   for f in fields)
        self._records = OrderedDict(((r.pipeline_name, r.from_study), r)
                                    for r in records)

    def __eq__(self, other):
        if not (isinstance(other, type(self)) or
                isinstance(self, type(other))):
            return False
        return (self._filesets == other._filesets and
                self._fields == other._fields)

    def __hash__(self):
        return hash(tuple(self.filesets)) ^ hash(tuple(self._fields))

    def __contains__(self, item):
        if item.is_fileset:
            return (item.name, item.from_study) in self._filesets

    @property
    def filesets(self):
        return self._filesets.values()

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

    def fileset(self, name, study=None):
        try:
            return self._filesets[(name, study)]
        except KeyError:
            available = [d.name for d in self.filesets
                         if d.from_study == study]
            other_studies = [(d.from_study if d.from_study is not None
                              else '<root>')
                             for d in self.filesets
                             if d.name == name]
            if other_studies:
                msg = (". NB: matching fileset(s) found for '{}' study(ies) "
                       "('{}')".format(name, "', '".join(other_studies)))
            else:
                msg = ''
            raise ArcanaNameError(
                name,
                ("{} doesn't have a fileset named '{}' {}"
                   "(available '{}'){}"
                   .format(self, name,
                           ("for study '{}'".format(study)
                            if study is not None else ''),
                           "', '".join(available), msg)))

    def field(self, name, study=None):
        try:
            return self._fields[(name, study)]
        except KeyError:
            available = [d.name for d in self.fields
                         if d.from_study == study]
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
                name, ("{} doesn't have a field named '{}' "
                       "(available '{}')"
                       .format(
                           self, name,
                           ("for study '{}'".format(study)
                            if study is not None else ''),
                           "', '".join(available), msg)))

    def record(self, name, study):
        try:
            return self._records[(name, study)]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a provenance record for pipeline '{}' "
                       "for '{}' study".format(self, name, study)))

    @property
    def data(self):
        return chain(self.filesets, self.fields)

    def __ne__(self, other):
        return not (self == other)

    def find_mismatch(self, other, indent=''):
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

    def check_provenance(self, pipeline, ignore_versions=False):
        from_study = pipeline.study.name

        def getchecksums(node, spec):
            """
            Extract the checksum or value for the item in the given node
            corresponding to the given spec
            """
            if spec.is_fileset:
                checksum = node.fileset(spec.name, from_study).checksums
            else:
                checksum = node.field(spec.name, from_study).value
            if checksum is None:
                raise ArcanaMissingDataException(
                    "The item corresponding to {} in {} does not exist, which "
                    "was expected to exist as an input or output of {}"
                    .format(spec, node, pipeline))
            return checksum

        exp_inputs = {}
        # Get checksums/values of all inputs that would have been used in
        # previous runs of an equivalent pipeline to compare with that saved
        # in provenance to see if any have been updated.
        for inpt in pipeline.inputs:
            # Get iterfields present in the input that aren't in this node
            # and need to be joined
            input_iterfields = pipeline.iterfields(inpt.frequency)
            output_iterfields = pipeline.iterfields(self.frequency)
            join_iterfields = input_iterfields - output_iterfields
            if not join_iterfields:
                # No iterfields to join so we can just extract the checksums
                # directly
                checksums = getchecksums(self, inpt)
            elif len(join_iterfields) == 1:
                # We need to join one iterfield, across all sub-nodes of the
                # current node or all nodes if the current node doesn't
                # iterate over that iterfield
                if input_iterfields & output_iterfields:
                    base_node = self
                else:
                    base_node = self.parent
                checksums = [
                    getchecksums(n, inpt)
                    for n in base_node.nodes(
                        pipeline.study.freq_from_iterfields(join_iterfields))]
            else:
                assert isinstance(self,
                                  Tree) and inpt.frequency == 'per_session'
                checksums
                exp_inputs[inpt.name] = [
                    [getchecksums(s, inpt) for s in subj.sessions]
                    for subj in self.subjects]
        # Get checksums/value for all outputs of the pipeline. We are assuming
        # that they exist here (otherwise they will be None)
        exp_outputs = {o.name: getchecksums(self, o) for o in pipeline.outputs}
        expected_record = pipeline.provenance().record(
            exp_inputs, exp_outputs, self.frequency, self.subject_id,
            self.visit_id)
        record = self.record(pipeline.name, pipeline.study.name)
        if not record.matches(expected_record, ignore_versions):
            raise ArcanaProvenanceRecordMismatchError(
                "Provenance recorded for '{}' pipeline in {} does not match "
                "that of requested pipeline, set reprocess flag == True to "
                "overwrite:\n{}".format(
                    pipeline.name, self,
                    record.find_mismatch(expected_record, indent='  ',
                                         ignore_versions=ignore_versions)))


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

    def __init__(self, subjects, visits, filesets=None, fields=None,
                 records=None, fill_subjects=None, fill_visits=None, **kwargs):  # @UnusedVariable @IgnorePep8
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

    def __eq__(self, other):
        return (super(Tree, self).__eq__(other) and
                self._subjects == other._subjects and
                self._visits == other._visits)

    def __hash__(self):
        return (TreeNode.__hash_(self) ^
                hash(tuple(self.subjects)) ^
                hash(tuple(self._visits)))

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
    def subject_ids(self):
        return self._subjects.keys()

    @property
    def visit_ids(self):
        return self._visits.keys()

    @property
    def session_ids(self):
        return ((s.subject_id, s.visit_id) for s in sessions)

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
            return self._subjects[id]
        except KeyError:
            raise ArcanaNameError(
                id, ("{} doesn't have a subject named '{}'"
                       .format(self, id)))

    def visit(self, id):  # @ReservedAssignment
        try:
            return self._visits[id]
        except KeyError:
            raise ArcanaNameError(
                id, ("{} doesn't have a visit named '{}'"
                       .format(self, id)))

    def session(self, subject_id, visit_id):
        return self.subject(subject_id).session(visit_id)

    def __iter__(self):
        return self.nodes()

    def nodes(self, frequency=None):
        """
        Returns an iterator over all nodes in the tree

        Parameters
        ----------
        frequency : str | None
            The frequency of the nodes to iterate over. If None all
            frequencies are returned
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
        if frequency is not None and frequency != 'per_session':
            raise ArcanaError(
                "Subjects only have 'per_session' nodes")
        return self.sessions

    @property
    def visit_ids(self):
        return self._sessions.values()

    def session(self, visit_id):
        try:
            return self._sessions[visit_id]
        except KeyError:
            raise ArcanaNameError(
                visit_id, ("{} doesn't have a session named '{}'"
                           .format(self, visit_id)))

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
        if frequency is not None and frequency != 'per_session':
            raise ArcanaError(
                "Subjects only have 'per_session' nodes")
        return self.sessions

    def session(self, subject_id):
        try:
            return self._sessions[subject_id]
        except KeyError:
            raise ArcanaNameError(
                subject_id, ("{} doesn't have a session named '{}'"
                             .format(self, subject_id)))

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
        raise ArcanaError(
            "Sessions don't have nodes")

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
