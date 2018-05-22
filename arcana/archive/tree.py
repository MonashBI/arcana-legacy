from itertools import chain
from arcana.exception import ArcanaNameError


class Project(object):

    def __init__(self, subjects, visits, datasets=None, fields=None):
        if datasets is None:
            datasets = []
        if fields is None:
            fields = []
        self._subjects = {s.id: s for s in subjects}
        self._visits = {v.id: v for v in visits}
        self._datasets = {d.name: d for d in datasets}
        self._fields = {f.name: f for f in fields}

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
                name, ("{} doesn't have a dataset named '{}'"
                       .format(self, name)))

    def field(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}'"
                       .format(self, name)))

    @property
    def data(self):
        return chain(self.datasets, self.fields)

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

    @property
    def data_names(self):
        return (d.name for d in self.data)

    def __eq__(self, other):
        if not isinstance(other, Project):
            return False
        return (self._subjects == other._subjects and
                self._visits == other._visits and
                self._datasets == other._datasets and
                self._fields == other._fields)

    def find_mismatch(self, other, indent=''):
        """
        Used in debugging unittests
        """
        if self != other:
            mismatch = "\n{}Project".format(indent)
        else:
            mismatch = ''
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

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Project(num_subjects={}, num_visits={}, "
                "num_datasets={}, num_fields={})".format(
                    len(list(self.subjects)),
                    len(list(self.visits)),
                    len(list(self.datasets)), len(list(self.fields))))


class Subject(object):
    """
    Holds a subject id and a list of sessions
    """

    def __init__(self, subject_id, sessions, datasets=None,
                 fields=None):
        if datasets is None:
            datasets = []
        if fields is None:
            fields = []
        self._id = subject_id
        self._sessions = {s.visit_id: s for s in sessions}
        self._datasets = {d.name: d for d in datasets}
        self._fields = {f.name: f for f in fields}
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
                name, ("{} doesn't have a dataset named '{}'"
                       .format(self, name)))

    def field(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}'"
                       .format(self, name)))

    @property
    def data(self):
        return chain(self.datasets, self.fields)

    @property
    def data_names(self):
        return (d.name for d in self.data)

    def __eq__(self, other):
        if not isinstance(other, Subject):
            return False
        return (self._id == other._id and
                self._sessions == other._sessions and
                self._datasets == other._datasets and
                self._fields == other._fields)

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}Subject '{}' != '{}'".format(
                indent, self.id, other.id)
        else:
            mismatch = ''
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

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Subject(id={}, num_sessions={})"
                .format(self._id, len(self._sessions)))


class Visit(object):
    """
    Holds a subject id and a list of sessions
    """

    def __init__(self, visit_id, sessions, datasets=None, fields=None):
        if datasets is None:
            datasets = []
        if fields is None:
            fields = []
        self._id = visit_id
        self._sessions = {s.subject_id: s for s in sessions}
        self._datasets = {d.name: d for d in datasets}
        self._fields = {f.name: f for f in fields}
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
                name, ("{} doesn't have a dataset named '{}'"
                       .format(self, name)))

    def field(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}'"
                       .format(self, name)))

    @property
    def data(self):
        return chain(self.datasets, self.fields)

    @property
    def data_names(self):
        return (d.name for d in self.data)

    def __eq__(self, other):
        if not isinstance(other, Visit):
            return False
        return (self._id == other._id and
                self._sessions == other._sessions and
                self._datasets == other._datasets and
                self._fields == other._fields)

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}Visit '{}' != '{}'".format(
                indent, self.id, other.id)
        else:
            mismatch = ''
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
                         '\n{indent}    self={}\n{indent}    other={}'
                         .format(len(list(self.fields)),
                                 len(list(other.fields)),
                                 list(self.fields),
                                 list(other.fields),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.fields, other.fields):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Visit(id={}, num_sessions={})".format(self._id,
                                                      len(self._sessions))


class Session(object):
    """
    Holds the session id and the list of datasets loaded from it

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
        if datasets is None:
            datasets = []
        if fields is None:
            fields = []
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._datasets = {d.name: d for d in datasets}
        self._fields = {f.name: f for f in fields}
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
                name, ("{} doesn't have a dataset named '{}'"
                       .format(self, name)))

    def field(self, name):
        try:
            return self._fields[name]
        except KeyError:
            raise ArcanaNameError(
                name, ("{} doesn't have a field named '{}'"
                       .format(self, name)))

    @property
    def data(self):
        return chain(self.datasets, self.fields)

    @property
    def data_names(self):
        return (d.name for d in self.data)

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
        if not isinstance(other, Session):
            return False
        return (self.subject_id == other.subject_id and
                self.visit_id == other.visit_id and
                self.datasets == other.datasets and
                self.fields == other.fields and
                self.derived == other.derived)

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}Session '{}-{}' != '{}-{}'".format(
                indent, self.subject_id, self.visit_id,
                other.subject_id, other.visit_id)
        else:
            mismatch = ''
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
        if len(list(self.datasets)) != len(list(other.datasets)):
            mismatch += ('\n{indent}mismatching dataset lengths '
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
            mismatch += ('\n{indent}mismatching field lengths '
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

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return ("Session(subject_id='{}', visit_id='{}', num_datasets={}, "
                "num_fields={}, derived={})".format(
                    self.subject_id, self.visit_id, len(self._datasets),
                    len(self._fields), self.derived))
