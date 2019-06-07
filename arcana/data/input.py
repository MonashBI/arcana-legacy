from builtins import object
from past.builtins import basestring
import re
from copy import copy
from itertools import chain
from arcana.exceptions import (
    ArcanaUsageError, ArcanaInputError,
    ArcanaInputMissingMatchError, ArcanaNotBoundToStudyError)
from .base import BaseFileset, BaseField
from .item import Fileset, Field
from .collection import FilesetCollection, FieldCollection


class BaseInput(object):
    """
    Base class for Fileset and Field Input classes
    """

    def __init__(self, pattern, is_regex, order, from_study,
                 skip_missing=False, drop_if_missing=False,
                 fallback_to_default=False, repository=None,
                 study_=None, collection_=None):
        self._pattern = pattern
        self._is_regex = is_regex
        self._order = order
        self._from_study = from_study
        self._repository = repository
        self._skip_missing = skip_missing
        self._drop_if_missing = drop_if_missing
        self._fallback_to_default = fallback_to_default
        if skip_missing and fallback_to_default:
            raise ArcanaUsageError(
                "Cannot provide both mutually exclusive 'skip_missing' and "
                "'fallback_to_default' flags to {}".format(self))
        # Set when fallback_to_default is True and there are missing matches
        self._derivable = False
        self._fallback = None
        # study_ and collection_ are not intended to be provided to __init__
        # except when recreating when using initkwargs
        self._study = study_
        self._collection = collection_

    def __eq__(self, other):
        return (self.from_study == other.from_study and
                self.pattern == other.pattern and
                self.is_regex == other.is_regex and
                self.order == other.order and
                self._repository == other._repository and
                self._skip_missing == other._skip_missing and
                self._drop_if_missing == other._drop_if_missing and
                self._fallback_to_default == other._fallback_to_default)

    def __hash__(self):
        return (hash(self.from_study) ^
                hash(self.pattern) ^
                hash(self.is_regex) ^
                hash(self.order) ^
                hash(self._repository) ^
                hash(self._skip_missing) ^
                hash(self._drop_if_missing) ^
                hash(self._fallback_to_default))

    def initkwargs(self):
        dct = {}
        dct['from_study'] = self.from_study
        dct['pattern'] = self.pattern
        dct['order'] = self.order
        dct['is_regex'] = self.is_regex
        dct['study_'] = self._study
        dct['collection_'] = self._collection
        dct['skip_missing'] = self._skip_missing
        dct['drop_if_missing'] = self._drop_if_missing
        dct['fallback_to_default'] = self._fallback_to_default
        return dct

    @property
    def pattern(self):
        return self._pattern

    @property
    def spec_name(self):
        return self.name

    @property
    def derived(self):
        return self._from_study is not None

    @property
    def derivable(self):
        return self._derivable

    @property
    def from_study(self):
        return self._from_study

    @property
    def skip_missing(self):
        return self._skip_missing

    @property
    def drop_if_missing(self):
        return self._drop_if_missing

    @property
    def fallback_to_default(self):
        return self._fallback_to_default

    @property
    def study(self):
        if self._study is None:
            raise ArcanaNotBoundToStudyError(
                "{} is not bound to a study".format(self))
        return self._study

    @property
    def repository(self):
        if self._repository is None:
            if self._study is None:
                raise ArcanaUsageError(
                    "Cannot access repository of {} as it wasn't explicitly "
                    "provided and Input hasn't been bound to a study"
                    .format(self))
            repo = self._study.repository
        else:
            repo = self._repository
        return repo

    @property
    def collection(self):
        if self._collection is None:
            raise ArcanaNotBoundToStudyError(
                "{} has not been bound to a study".format(self))
        return self._collection

    @property
    def pipeline_getter(self):
        "For duck-typing with *Spec types"
        if not self.derivable:
            raise ArcanaUsageError(
                "There is no pipeline getter for {} because it doesn't "
                "fallback to a derived spec".format(self))
        return self._fallback.pipeline_getter

    @property
    def is_regex(self):
        return self._is_regex

    @property
    def order(self):
        return self._order

    def bind(self, study, spec_name=None, **kwargs):
        if spec_name is None:
            spec_name = self.spec_name
        if self._study == study:
            bound = self
        else:
            # Create copy and set study
            bound = copy(self)
            bound._study = study
            spec = study.data_spec(spec_name)
            # Use the default study repository if not explicitly
            # provided to match
            if self.fallback_to_default:
                if spec.derived:
                    bound._derivable = True
                elif spec.default is not None:
                    raise ArcanaUsageError(
                        "Cannot fallback to default for '{}' spec in {} as it "
                        " is not derived and doesn't have a default"
                        .format(self.name, study))
                # We don't want to add the bound copy to the study as that will
                # override the selector so we bind the fallback to the study
                # from the spec explicitly
                bound._fallback = spec.bind(study)
            # Match against tree
            if self._repository is None:
                repository = study.repository
            else:
                repository = self._repository
            with repository:
                tree = repository.cached_tree(
                    subject_ids=study.subject_ids,
                    visit_ids=study.visit_ids)
                bound._collection = bound.match(
                    tree, valid_formats=getattr(spec, 'valid_formats', None),
                    **kwargs)
        return bound

    @property
    def prefixed_name(self):
        return self.name

    def nodes(self, tree):
        # Run the match against the tree
        if self.frequency == 'per_session':
            nodes = chain(*(s.sessions for s in tree.subjects))
        elif self.frequency == 'per_subject':
            nodes = tree.subjects
        elif self.frequency == 'per_visit':
            nodes = tree.visits
        elif self.frequency == 'per_study':
            nodes = [tree]
        else:
            assert False, "Unrecognised frequency '{}'".format(self.frequency)
        return nodes

    def _match(self, tree, item_cls, **kwargs):
        matches = []
        errors = []
        for node in self.nodes(tree):
            try:
                try:
                    matches.append(self.match_node(node, **kwargs))
                except ArcanaInputMissingMatchError as e:
                    if self._fallback is not None:
                        matches.append(self._fallback.collection.item(
                            subject_id=node.subject_id,
                            visit_id=node.visit_id))
                    elif self.skip_missing:
                        # Insert a non-existant item placeholder in-place of
                        # the the missing item
                        matches.append(item_cls(
                            self.name,
                            frequency=self.frequency,
                            subject_id=node.subject_id,
                            visit_id=node.visit_id,
                            repository=self.study.repository,
                            from_study=self.from_study,
                            exists=False,
                            **self._specific_kwargs))
                    else:
                        raise e
            except ArcanaInputError as e:
                errors.append(e)
        # Collate potentially multiple errors into a single error message
        if errors:
            if all(isinstance(e, ArcanaInputMissingMatchError)
                   for e in errors):
                ErrorClass = ArcanaInputMissingMatchError
            else:
                ErrorClass = ArcanaInputError
            raise ErrorClass('\n'.join(str(e) for e in errors))
        return matches

    def match_node(self, node, **kwargs):
        # Get names matching pattern
        matches = self._filtered_matches(node, **kwargs)
        # Filter matches by study name
        study_matches = [d for d in matches
                         if d.from_study == self.from_study]
        # Select the fileset from the matches
        if not study_matches:
            raise ArcanaInputMissingMatchError(
                "No matches found for {} in {} for study {}, however, found {}"
                .format(self, node, self.from_study,
                        ', '.join(str(m) for m in matches)))
        elif self.order is not None:
            try:
                match = study_matches[self.order]
            except IndexError:
                raise ArcanaInputMissingMatchError(
                    "Did not find {} named data matching pattern {}"
                    " (found {}) in {}".format(self.order, self.pattern,
                                               len(matches), node))
        elif len(study_matches) == 1:
            match = study_matches[0]
        else:
            raise ArcanaInputError(
                "Found multiple matches for {} in {} ({})"
                .format(self, node, ', '.join(str(m) for m in study_matches)))
        return match


class InputFilesets(BaseInput, BaseFileset):
    """
    A pattern that describes a single fileset (typically acquired
    rather than generated but not necessarily) within each session.

    Parameters
    ----------
    spec_name : str
        The name of the fileset spec to match against
    format : FileFormat
        The file format used to store the fileset.
    pattern : str
        A regex pattern to match the fileset names with. Must match
        one and only one fileset per <frequency>. If None, the name
        is used instead.
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    id : int | None
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The ID of the fileset within the
        session.
    order : int | None
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The order of the fileset within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    dicom_tags : dct(str | str)
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The provided DICOM values dicom
        header values must match exactly.
    from_study : str
        The name of the study that generated the derived fileset to match.
        Is used to determine the location of the filesets in the
        repository as the derived filesets and fields are grouped by
        the name of the study that generated them.
    skip_missing : bool
        If there is no fileset matching the selector for a node then pipelines
        that use it as an input, including downstream pipelines, will not be
        run for that node
    drop_if_missing : bool
        If there are missing filesets then drop the selector from the study
        input. Useful in the case where you want to provide selectors for the
        a list of inputs which may or may not be acquired for a range of
        studies
    fallback_to_default : bool
        If there is no fileset matching the selection for a node
        and corresponding data spec has a default or is a derived spec
        then fallback to the default or generate the derivative.
    repository : Repository | None
        The repository to draw the matches from, if not the main repository
        that is used to store the products of the current study.
    acceptable_quality : str | list[str] | None
        An acceptable quality label, or list thereof, to accept, i.e. if a
        fileset's quality label is not in the list it will be ignored. If a
        scan wasn't labelled the value of its qualtiy will be None.
    """

    is_spec = False

    def __init__(self, spec_name, pattern=None, format=None, # @ReservedAssignment @IgnorePep8
                 frequency='per_session', id=None,  # @ReservedAssignment
                 order=None, dicom_tags=None, is_regex=False, from_study=None,
                 skip_missing=False, drop_if_missing=False,
                 fallback_to_default=False, repository=None,
                 acceptable_quality=None,
                 study_=None, collection_=None):
        BaseFileset.__init__(self, spec_name, format, frequency)
        BaseInput.__init__(self, pattern, is_regex, order,
                              from_study, skip_missing, drop_if_missing,
                              fallback_to_default, repository, study_,
                              collection_)
        if dicom_tags is not None and format.name != 'dicom':
            raise ArcanaUsageError(
                "Cannot use 'dicom_tags' kwarg with non-DICOM "
                "format ({})".format(format))
        self._dicom_tags = dicom_tags
        if order is not None and id is not None:
            raise ArcanaUsageError(
                "Cannot provide both 'order' and 'id' to a fileset"
                "match")
        self._id = str(id) if id is not None else id
        if isinstance(acceptable_quality, basestring):
            acceptable_quality = (acceptable_quality,)
        elif acceptable_quality is not None:
            acceptable_quality = tuple(acceptable_quality)
        self._acceptable_quality = acceptable_quality

    def __eq__(self, other):
        return (BaseFileset.__eq__(self, other) and
                BaseInput.__eq__(self, other) and
                self.dicom_tags == other.dicom_tags and
                self.id == other.id and
                self._acceptable_quality == other._acceptable_quality)

    def __hash__(self):
        return (BaseFileset.__hash__(self) ^
                BaseInput.__hash__(self) ^
                hash(self.dicom_tags) ^
                hash(self.id) ^
                hash(self._acceptable_quality))

    def initkwargs(self):
        dct = BaseFileset.initkwargs(self)
        dct.update(BaseInput.initkwargs(self))
        dct['dicom_tags'] = self.dicom_tags
        dct['id'] = self.id
        dct['acceptable_quality'] = self.acceptable_quality
        return dct

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency={}, pattern={}, "
                "is_regex={}, order={}, id={}, dicom_tags={}, "
                "from_study={}, acceptable_quality={})"
                .format(self.__class__.__name__, self.name, self._format,
                        self.frequency, self._pattern, self.is_regex,
                        self.order, self.id, self.dicom_tags,
                        self._from_study, self._acceptable_quality))

    def match(self, tree, valid_formats=None, **kwargs):
        if self.format is not None:
            candidate_formats = [self.format]
        else:
            if valid_formats is None:
                raise ArcanaUsageError(
                    "'valid_formats' need to be provided to the 'match' "
                    "method if the InputFilesets ({}) doesn't specify a format"
                    .format(self))
            candidate_formats = valid_formats
        # Run the match against the tree
        return FilesetCollection(self.name,
                                 self._match(tree, Fileset, **kwargs),
                                 frequency=self.frequency,
                                 candidate_formats=candidate_formats)

    @property
    def id(self):
        return self._id

    @property
    def acceptable_quality(self):
        return self._acceptable_quality

    @property
    def format(self):
        if self._format is None:
            try:
                format = self.collection.format  # @ReservedAssignment
            except ArcanaNotBoundToStudyError:
                format = None  # @ReservedAssignment
        else:
            format = self._format  # @ReservedAssignment
        return format

    @property
    def dicom_tags(self):
        return self._dicom_tags

    def _filtered_matches(self, node, valid_formats=None, **kwargs):  # @UnusedVariable @IgnorePep8
        if self.format is not None:
            valid_formats = [self.format]
        if self.pattern is not None:
            if self.is_regex:
                pattern_re = re.compile(self.pattern)
                matches = [f for f in node.filesets
                           if pattern_re.match(f.basename)]
            else:
                matches = [f for f in node.filesets
                           if f.basename == self.pattern]
        else:
            matches = list(node.filesets)
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}, found:\n{}"
                .format(self, node,
                        '\n'.join(str(f) for f in node.filesets)))
        if self.acceptable_quality is not None:
            filtered = [f for f in matches
                        if f.quality in self.acceptable_quality]
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find filesets names matching pattern {} "
                    "with an acceptable quality {} (found {}) in {}".format(
                        self.pattern, self.acceptable_quality,
                        ', '.join(str(m) for m in matches), node))
            matches = filtered
        if self.id is not None:
            filtered = [d for d in matches if d.id == self.id]
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find filesets names matching pattern {} "
                    "with an id of {} (found {}) in {}".format(
                        self.pattern, self.id,
                        ', '.join(str(m) for m in matches), node))
            matches = filtered
        if valid_formats is not None:
            format_matches = [
                m for m in matches if any(f.matches(m) for f in valid_formats)]
            if not format_matches:
                for f in matches:
                    self.format.matches(f)
                raise ArcanaInputMissingMatchError(
                    "Did not find any filesets that match the file format "
                    "specified by {} in {}, found:\n{}"
                    .format(self, node, '\n'.join(str(f) for f in matches)))
            matches = format_matches
        # Filter matches by dicom tags
        if self.dicom_tags is not None:
            filtered = []
            for fileset in matches:
                keys, ref_values = zip(*self.dicom_tags.items())
                values = tuple(self.format.dicom_values(fileset, keys))
                if ref_values == values:
                    filtered.append(fileset)
            if not filtered:
                raise ArcanaInputMissingMatchError(
                    "Did not find filesets names matching pattern {}"
                    "that matched DICOM tags {} (found {}) in {}"
                    .format(self.pattern, self.dicom_tags,
                            ', '.join(str(m) for m in matches), node))
            matches = filtered
        return matches

    def cache(self):
        """
        Forces the cache of the input fileset. Can be useful for before running
        a workflow that will use many concurrent jobs/processes to source data
        from remote repository, to force the download to be done linearly and
        avoid DOSing the host
        """
        for item in self.collection:
            if item.exists:
                item.get()

    @property
    def _specific_kwargs(self):
        return {'format': self.format}


class InputFields(BaseInput, BaseField):
    """
    A pattern that matches a single field (typically acquired rather than
    generated but not necessarily) in each session.

    Parameters
    ----------
    spec_name : str
        The name of the field spec to match against.
    pattern : str
        A regex pattern to match the field names with. Must match
        one and only one fileset per <frequency>. If None, the name
        is used instead.
    dtype : type | None
        The datatype of the value. Can be one of (float, int, str). If None
        then the dtype is taken from the FieldSpec that it is bound to
    is_regex : bool
        Flags whether the pattern is a regular expression or not
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_study',
        specifying whether the fileset is present for each session, subject,
        visit or project.
    order : int | None
        To be used to distinguish multiple filesets that match the
        pattern in the same session. The order of the fileset within the
        session. Based on the scan ID but is more robust to small
        changes to the IDs within the session if for example there are
        two scans of the same type taken before and after a task.
    from_study : str
        The name of the study that generated the derived field to match.
        Is used to determine the location of the fields in the
        repository as the derived filesets and fields are grouped by
        the name of the study that generated them.
    skip_missing : bool
        If there is no field matching the selector for a node then pipelines
        that use it as an input, including downstream pipelines, will not be
        run for that node
    drop_if_missing : bool
        If there are missing filesets then drop the selector from the study
        input. Useful in the case where you want to provide selectors for the
        a list of inputs which may or may not be acquired for a range of
        studies
    fallback_to_default : bool
        If the there is no fileset/field matching the selection for a node
        and corresponding data spec has a default or is a derived spec,
        then fallback to the default or generate the derivative.
    repository : Repository | None
        The repository to draw the matches from, if not the main repository
        that is used to store the products of the current study.
    """

    is_spec = False

    def __init__(self, spec_name, pattern, dtype=None, frequency='per_session',
                 order=None, is_regex=False, from_study=None,
                 skip_missing=False, drop_if_missing=False,
                 fallback_to_default=False, repository=None, study_=None,
                 collection_=None):
        BaseField.__init__(self, spec_name, dtype, frequency)
        BaseInput.__init__(self, pattern, is_regex, order,
                              from_study, skip_missing, drop_if_missing,
                              fallback_to_default, repository, study_,
                              collection_)

    def __eq__(self, other):
        return (BaseField.__eq__(self, other) and
                BaseInput.__eq__(self, other))

    def match(self, tree, **kwargs):
        # Run the match against the tree
        return FieldCollection(self.name,
                               self._match(tree, Field, **kwargs),
                               frequency=self.frequency,
                               dtype=self.dtype)

    @property
    def dtype(self):
        if self._dtype is None:
            try:
                dtype = self.study.data_spec(self.name).dtype
            except ArcanaNotBoundToStudyError:
                dtype = None
        else:
            dtype = self._dtype
        return dtype

    def __hash__(self):
        return (BaseField.__hash__(self) ^ BaseInput.__hash__(self))

    def initkwargs(self):
        dct = BaseField.initkwargs(self)
        dct.update(BaseInput.initkwargs(self))
        return dct

    def _filtered_matches(self, node, **kwargs):  # @UnusedVariable
        if self.is_regex:
            pattern_re = re.compile(self.pattern)
            matches = [f for f in node.fields
                       if pattern_re.match(f.name)]
        else:
            matches = [f for f in node.fields
                       if f.name == self.pattern]
        if self.from_study is not None:
            matches = [f for f in matches
                       if f.from_study == self.from_study]
        if not matches:
            raise ArcanaInputMissingMatchError(
                "Did not find any matches for {} in {}, found:\n{}"
                .format(self, node, '\n'.join(f.name for f in node.fields)))
        return matches

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency={}, pattern={}, "
                "is_regex={}, order={}, from_study={})"
                .format(self.__class__.__name__, self.name, self._dtype,
                        self.frequency, self._pattern, self.is_regex,
                        self.order, self._from_study))

    @property
    def _specific_kwargs(self):
        return {'dtype': self.dtype}
