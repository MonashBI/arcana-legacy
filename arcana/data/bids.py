from arcana.exception import ArcanaFilesetSelectorError, ArcanaUsageError
from .selector import FilesetSelector


class BidsAttrs(object):

    def __init__(self, type=None, modality=None, run=None, metadata=None,  # @ReservedAssignment @IgnorePep8
                 field_maps=None, bvec=None, bval=None, desc=None):
        self._type = type
        self._modality = modality
        self._run = run
        self._metadata = metadata
        self._bval = bval
        self._bvec = bvec
        self._field_maps = field_maps
        self._desc = desc

    @property
    def type(self):
        return self._type

    @property
    def mode(self):
        return self._mode

    @property
    def run(self):
        return self._run

    @property
    def metadata(self):
        return self._metadata

    @property
    def bval(self):
        return self._bval

    @property
    def bvec(self):
        return self._bvec

    @property
    def field_maps(self):
        return self._field_maps

    @property
    def desc(self):
        return self._desc


class BidsMatch(FilesetSelector):
    """
    A match object for matching filesets from their 'bids_attr'
    attribute

    Parameters
    ----------
    name : str
        Name of the fileset
    type : str
        Type of the fileset
    modality : str
        Modality of the filesets
    format : FileFormat
        The file format of the fileset to match
    run : int
        Run number of the fileset
    """

    def __init__(self, name, type, modality, format, run=None):  # @ReservedAssignment @IgnorePep8
        FilesetSelector.__init__(
            self, name, format, pattern=None, frequency='per_session',   # @ReservedAssignment @IgnorePep8
            id=None, order=run, dicom_tags=None, is_regex=False,
            from_study=None)
        self._type = type
        self._modality = modality
        self._run = run

    @property
    def type(self):
        return self._type

    @property
    def modality(self):
        return self._modality

    @property
    def run(self):
        return self.order

    def _filtered_matches(self, node):
        matches = [
            d for d in node.filesets
            if (d.bids_attr.entities['type'] == self.type and
                d.bids_attr.entities['modality'] == self.modality)]
        if not matches:
            raise ArcanaFilesetSelectorError(
                "No BIDS filesets for subject={}, visit={} match "
                "modality '{}' and type '{}' found:\n{}"
                .format(node.subject_id, node.visit_id, self.modality,
                        self.type, '\n'.join(
                            sorted(d.name for d in node.filesets))))
        return matches

    def __eq__(self, other):
        return (FilesetSelector.__eq__(self, other) and
                self.type == other.type and
                self.modality == other.modality and
                self.run == other.run)

    def __hash__(self):
        return (FilesetSelector.__hash__(self) ^
                hash(self.type) ^
                hash(self.modality) ^
                hash(self.run))

    def initkwargs(self):
        dct = FilesetSelector.initkwargs(self)
        dct['type'] = self.type
        dct['modality'] = self.modality
        dct['run'] = self.run
        return dct


class BidsAssociatedMatch(FilesetSelector):
    """
    A match object for matching BIDS filesets that are associated with
    another BIDS filesets (e.g. field-maps, bvecs, bvals)

    Parameters
    ----------
    name : str
        Name of the associated fileset
    primary_match : BidsMatch
        The primary fileset which the fileset to match is associated with
    associated : str
        The name of the association between the fileset to match and the
        primary fileset
    fieldmap_type : str
        Key of the return fieldmap dictionary (if association=='fieldmap'
    order : int
        Order of the fieldmap dictionary that you want to match
    """

    VALID_ASSOCIATIONS = ('fieldmap', 'bvec', 'bval')

    def __init__(self, name, primary_match, format, association,  # @ReservedAssignment @IgnorePep8
                 fieldmap_type=None, order=0):
        FilesetSelector.__init__(
            self, name, format, pattern=None, frequency='per_session',   # @ReservedAssignment @IgnorePep8
            id=None, order=order, dicom_tags=None, is_regex=False,
            from_study=None)
        self._primary_match = primary_match
        self._association = association
        if fieldmap_type is not None and association != 'fieldmap':
            raise ArcanaUsageError(
                "'fieldmap_type' (provided to '{}' match) "
                "is only valid for 'fieldmap' "
                "associations (not '{}')".format(name, association))
        self._fieldmap_type = fieldmap_type

    def __repr__(self):
        return ("{}(name={}, primary_match={}, format={}, association={}, "
                "fieldmap_type\{}, order={})".format(
                    self.name, self.primary_match, self.format,
                    self.association, self.fieldmap_type,
                    self.order))

    @property
    def primary_match(self):
        return self._primary_match

    @property
    def association(self):
        return self._association

    @property
    def fieldmap_type(self):
        return self._fieldmap_type

    def _bind_node(self, node):
        primary_match = self._primary_match._bind_node(node)
        layout = self.study.repository.layout
        if self._association == 'fieldmap':
            matches = layout.get_fieldmap(primary_match.path,
                                          return_list=True)
            try:
                match = matches[0]
            except IndexError:
                raise ArcanaFilesetSelectorError(
                    "Provided order to associated BIDS fileset match "
                    "{} is out of range")
        elif self._association == 'bvec':
            match = layout.get_bvec(primary_match.path)
        elif self._association == 'bval':
            match = layout.get_bval(primary_match.path)
        return match
        return matches

    def __eq__(self, other):
        return (FilesetSelector.__eq__(self, other) and
                self.primary_match == other.primary_match and
                self.association == other.association and
                self.fieldmap_type == other.fieldmap_type)

    def __hash__(self):
        return (FilesetSelector.__hash__(self) ^
                hash(self.primary_match) ^
                hash(self.association) ^
                hash(self.fieldmap_type))

    def initkwargs(self):
        dct = FilesetSelector.initkwargs(self)
        dct['primary_match'] = self.primary_match
        dct['association'] = self.association
        dct['fieldmap_type'] = self.fieldmap_type
        return dct
