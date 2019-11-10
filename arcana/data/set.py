class Dataset():

    def __init__(self, name, filters=None, subject_ids=None, visit_ids=None):
        self._name = name
        self._filters = filters
        self._subject_ids = subject_ids
        self._visit_ids = visit_ids

    @property
    def name(self):
        return self._name

    @property
    def filters(self):
        return self._filters

    @property
    def subject_ids(self):
        return self._subject_ids

    @property
    def visit_ids(self):
        return self._visit_ids
