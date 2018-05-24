from itertools import chain
from collections import OrderedDict
from arcana.exception import ArcanaNameError


class TreeNode(object):

    def __init__(self, datasets, fields):
        if datasets is None:
            datasets = []
        if fields is None:
            fields = []
        self._datasets = OrderedDict((d.name, d) for d in datasets)
        self._fields = OrderedDict((f.name, f) for f in fields)

    @property
    def datasets(self):
        return self._datasets.itervalues()

    @property
    def fields(self):
        return self._fields.itervalues()

    @property
    def dataset_names(self):
        return self._datasets.iterkeys()

    @property
    def field_names(self):
        return self._fields.iterkeys()

    def dataset(self, name):
        try:
            return self._datasets[name]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a dataset named '{}' "
                       "(available '{}')"
                       .format(self, name,
                               "', '".join(self.dataset_names))))

    def field(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}' "
                       "(available '{}')"
                       .format(self, name,
                               "', '".join(self.field_names))))

    @property
    def data(self):
        return chain(self.datasets, self.fields)

    @property
    def data_names(self):
        return (d.name for d in self.data)

    def __eq__(self, other):
        if not (isinstance(other, type(self)) or
                isinstance(self, type(other))):
            return False
        return (self._datasets == other._datasets and
                self._fields == other._fields)

    def __ne__(self, other):
        return not (self == other)

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}{}".format(indent, type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if len(list(self.datasets)) != len(list(other.datasets)):
            mismatch += ('\n{indent}mismatching summary dataset lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.datasets)),
                                 len(list(other.datasets)),
                                 list(self.datasets),
                                 list(other.datasets),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.datasets, other.datasets):
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


class Project(TreeNode):
    """
    Represents a project tree as stored in an archive

    Parameters
    ----------
    subjects : List[Subject]
        List of subjects
    visits : List[Visits]
        List of visits in the project across subjects
        (i.e. timepoint 1, 2, 3)
    datasets : List[Dataset]
        The datasets that belong to the project, i.e. of 'per_project'
        frequency
    fields : List[Field]
        The fields that belong to the project, i.e. of 'per_project'
        frequency
    """

    def __init__(self, subjects, visits, datasets=None, fields=None):
        TreeNode.__init__(self, datasets, fields)
        self._subjects = {s.id: s for s in subjects}
        self._visits = {v.id: v for v in visits}

    @property
    def subjects(self):
        return self._subjects.itervalues()

    @property
    def visits(self):
        return self._visits.itervalues()

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

    def nodes(self, frequency):
        if frequency == 'per_session':
            nodes = chain(*(s.sessions for s in self.subjects))
        elif frequency == 'per_subject':
            nodes = self.subjects
        elif frequency == 'per_visit':
            nodes = self.visits
        elif frequency == 'per_project':
            nodes = [self]
        else:
            assert False
        return nodes

    def __eq__(self, other):
        return (super(Project, self).__eq__(other) and
                self._subjects == other._subjects and
                self._visits == other._visits)

    def find_mismatch(self, other, indent=''):
        """
        Used in debugging unittests
        """
        mismatch = super(Project, self).find_mismatch(other, indent)
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
        return ("Project(num_subjects={}, num_visits={}, "
                "num_datasets={}, num_fields={})".format(
                    len(list(self.subjects)),
                    len(list(self.visits)),
                    len(list(self.datasets)), len(list(self.fields))))


class Subject(TreeNode):
    """
    Represents a subject as stored in an archive

    Parameters
    ----------
    subject_id : str
        The ID of the subject
    sessions : List[Session]
        The sessions in the subject
    datasets : List[Dataset]
        The datasets that belong to the subject, i.e. of 'per_subject'
        frequency
    fields : List[Field]
        The fields that belong to the subject, i.e. of 'per_subject'
        frequency
    """

    def __init__(self, subject_id, sessions, datasets=None,
                 fields=None):
        TreeNode.__init__(self, datasets, fields)
        self._id = subject_id
        self._sessions = {s.visit_id: s for s in sessions}
        for session in self.sessions:
            session.subject = self

    @property
    def id(self):
        return self._id

    def __lt__(self, other):
        return self._id < other._id

    @property
    def sessions(self):
        return self._sessions.itervalues()

    def session(self, visit_id):
        try:
            return self._sessions[visit_id]
        except KeyError:
            raise ArcanaNameError(
                visit_id, ("{} doesn't have a session named '{}'"
                           .format(self, visit_id)))

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other)and
                self._id == other._id and
                self._sessions == other._sessions)

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
    as stored in an archive

    Parameters
    ----------
    visit_id : str
        The ID of the visit
    sessions : List[Session]
        The sessions in the visit
    datasets : List[Dataset]
        The datasets that belong to the visit, i.e. of 'per_visit'
        frequency
    fields : List[Field]
        The fields that belong to the visit, i.e. of 'per_visit'
        frequency
    """

    def __init__(self, visit_id, sessions, datasets=None, fields=None):
        TreeNode.__init__(self, datasets, fields)
        self._id = visit_id
        self._sessions = {s.subject_id: s for s in sessions}
        for session in sessions:
            session.visit = self

    @property
    def id(self):
        return self._id

    def __lt__(self, other):
        return self._id < other._id

    @property
    def sessions(self):
        return self._sessions.itervalues()

    def session(self, subject_id):
        try:
            return self._sessions[subject_id]
        except KeyError:
            raise ArcanaNameError(
                subject_id, ("{} doesn't have a session named '{}'"
                             .format(self, subject_id)))

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other)and
                self._id == other._id and
                self._sessions == other._sessions)

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
    Represents a session stored in an archive

    Parameters
    ----------
    subject_id : str
        The subject ID of the session
    visit_id : str
        The visit ID of the session
    datasets : list(Dataset)
        The datasets found in the session
    derived : Session
        If derived scans are stored in a separate session, it is provided
        here
    """

    def __init__(self, subject_id, visit_id, datasets=None, fields=None,
                 derived=None):
        TreeNode.__init__(self, datasets, fields)
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._subject = None
        self._visit = None
        self._derived = derived

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def subject_id(self):
        return self._subject_id

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
    def derived(self):
        return self._derived

    @derived.setter
    def derived(self, derived):
        self._derived = derived

    @property
    def acquired(self):
        """True if the session contains acquired scans"""
        return not self._derived or self._derived is None

    @property
    def derived_dataset_names(self):
        datasets = (self.datasets
                    if self.derived is None else self.derived.datasets)
        return (d.name for d in datasets)

    @property
    def derived_field_names(self):
        fields = (self.fields
                  if self.derived is None else self.derived.fields)
        return (f.name for f in fields)

    @property
    def derived_data_names(self):
        return chain(self.derived_dataset_names,
                     self.derived_field_names)

    @property
    def all_dataset_names(self):
        return chain(self.dataset_names, self.derived_dataset_names)

    @property
    def all_field_names(self):
        return chain(self.field_names, self.derived_field_names)

    @property
    def all_data_names(self):
        return chain(self.data_names, self.derived_data_names)

    def __eq__(self, other):
        return (TreeNode.__eq__(self, other) and
                self.subject_id == other.subject_id and
                self.visit_id == other.visit_id and
                self.derived == other.derived)

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
        if self.derived != other.derived:
            mismatch += ('\n{}derived: self={} v other={}'
                         .format(sub_indent, self.derived,
                                 other.derived))
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Session(subject_id='{}', visit_id='{}', num_datasets={}, "
                "num_fields={}, derived={})".format(
                    self.subject_id, self.visit_id, len(self._datasets),
                    len(self._fields), self.derived))
