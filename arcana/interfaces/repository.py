from arcana.utils import ExitStack
from nipype.interfaces.base import (
    traits, DynamicTraitedSpec, Undefined, File, Directory,
    BaseInterface, isdefined)
from arcana.data import BaseField, BaseFileset
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX
import logging

logger = logging.getLogger('arcana')

PATH_TRAIT = traits.Either(File(exists=True), Directory(exists=True))
FIELD_TRAIT = traits.Either(traits.Int, traits.Float, traits.Str,
                            traits.List(traits.Int), traits.List(traits.Float),
                            traits.List(traits.Str))


class BaseRepositoryInterface(BaseInterface):
    """
    Parameters
    ----------
    infields : list of str
        Indicates the input fields to be dynamically created

    outfields: list of str
        Indicates output fields to be dynamically created

    See class examples for usage

    """

    def __init__(self, collections):
        super(BaseRepositoryInterface, self).__init__()
        collections = list(collections)  # Protect against iterators
        self.repositories = set(c.repository for c in collections
                                 if c.repository is not None)
        self.fileset_collections = [c for c in collections
                                    if isinstance(c, BaseFileset)]
        self.field_collections = [c for c in collections
                                  if isinstance(c, BaseField)]

    def __eq__(self, other):
        try:
            return (
                self.fileset_collections == other.fileset_collections and
                self.field_collections == other.field_collections)
        except AttributeError:
            return False

    def __repr__(self):
        return "{}(filesets={}, fields={})".format(
            type(self).__name__, self.fileset_collections,
            self.field_collections)

    def __ne__(self, other):
        return not self == other

    def _run_interface(self, runtime, *args, **kwargs):  # @UnusedVariable
        return runtime

    @classmethod
    def _add_trait(cls, spec, name, trait_type):
        spec.add_trait(name, trait_type)
        spec.trait_set(trait_change_notify=False, **{name: Undefined})
        # Access the trait (not sure why but this is done in add_traits
        # so I have also done it here
        getattr(spec, name)

    @classmethod
    def field_trait(cls, field):
        if field.array:
            trait = traits.List(field.dtype)
        else:
            trait = field.dtype
        return trait


class BaseRepositorySpec(DynamicTraitedSpec):
    """
    Base class for input and output specifications for repository source
    and sink interfaces
    """
    subject_id = traits.Str(desc="The subject ID")
    visit_id = traits.Str(desc="The visit ID")


class RepositorySourceInputSpec(BaseRepositorySpec):
    """
    Input specification for repository source interfaces.
    """
    prereqs = traits.List(
        desc=("A list of lists of iterator IDs used in prerequisite pipelines."
              " Only passed here to ensure that prerequisites are processed "
              "before this source is run (so that their outputs exist in the "
              "repository)"))


class RepositorySourceOutputSpec(BaseRepositorySpec):
    """
    Output specification for repository source interfaces.
    """
    checksums = traits.Dict(
        desc=("Checksums of input filesets (values of fields)"))


class RepositorySource(BaseRepositoryInterface):
    """
    Parameters
    ----------
    filesets: list
        List of all filesets to be extracted from the repository
    fields: list
        List of all the fields that are to be extracted from the repository
    """

    input_spec = RepositorySourceInputSpec
    output_spec = RepositorySourceOutputSpec
    _always_run = True

    def _outputs(self):
        outputs = super(RepositorySource, self)._outputs()
        # Add output filesets
        for fileset_collection in self.fileset_collections:
            self._add_trait(outputs,
                            fileset_collection.name + PATH_SUFFIX,
                            PATH_TRAIT)
        # Add output fields
        for field_collection in self.field_collections:

            self._add_trait(outputs,
                            field_collection.name + FIELD_SUFFIX,
                            self.field_trait(field_collection))
        return outputs

    def _list_outputs(self):
        # Directory that holds session-specific
        outputs = self.output_spec().get()
        subject_id = (self.inputs.subject_id
                      if isdefined(self.inputs.subject_id) else None)
        visit_id = (self.inputs.visit_id
                    if isdefined(self.inputs.visit_id) else None)
        outputs['subject_id'] = self.inputs.subject_id
        outputs['visit_id'] = self.inputs.visit_id
        # Source filesets
        checksums = outputs['checksums'] = {}
        with ExitStack() as stack:
            # Connect to set of repositories that the collections come from
            for repository in self.repositories:
                stack.enter_context(repository)
            for fileset_collection in self.fileset_collections:
                fileset = fileset_collection.item(subject_id, visit_id)
                fileset.get()
                outputs[fileset_collection.name + PATH_SUFFIX] = fileset.path
                checksums[fileset_collection.name] = fileset.checksums
            for field_collection in self.field_collections:
                field = field_collection.item(subject_id, visit_id)
                field.get()
                outputs[field_collection.name + FIELD_SUFFIX] = field.value
                checksums[field_collection.name] = field.value
        return outputs


class RepositorySinkInputSpec(BaseRepositorySpec):

    per_session_checksums = traits.Dict(
        {}, desc=("The checksums or values of per-session input filesets or "
                  "fields, respectively, that have been used by the pipeline"),
        usedefault=True)
    per_subject_checksums = traits.Dict(
        {}, desc=("The checksums or values of per-subject input filesets or "
                  "fields, respectively, that have been used by the pipeline"),
        usedefault=True)
    per_visit_checksums = traits.Dict(
        {}, desc=("The checksums or values of per-visit input filesets or "
                  "fields, respectively, that have been used by the pipeline"),
        usedefault=True)
    per_study_checksums = traits.Dict(
        {}, desc=("The checksums or values of per-study input filesets or "
                  "fields, respectively, that have been used by the pipeline"),
        usedefault=True)


class RepositorySinkOutputSpec(BaseRepositorySpec):

    files = traits.List(PATH_TRAIT, desc='Output filesets')

    fields = traits.List(
        traits.Tuple(traits.Str, FIELD_TRAIT), desc='Output fields')
    combined = traits.List(
        traits.Either(PATH_TRAIT, traits.Tuple(traits.Str, FIELD_TRAIT)),
        desc="Combined fileset and field outputs")


class RepositorySink(BaseRepositoryInterface):
    """
    Interface used to sink derivatives into the output repository

    Parameters
    ----------
    collections : *Collection
        The collections of Field and Fileset objects to insert into the
        outputs repositor(y|ies)
    provenance : arcana.provenance.PipelineRecord
        The pipeline provenance record (as opposed to the session or subject|
        visit|study-summary specific record that contains field values and
        file-set checksums for relevant inputs and outputs)
    """

    input_spec = RepositorySinkInputSpec
    output_spec = RepositorySinkOutputSpec

    def __init__(self, collections, provenance, frequencies):
        super(RepositorySink, self).__init__(collections)
        # Add input filesets
        for fileset_collection in self.fileset_collections:
            self._add_trait(self.inputs,
                            fileset_collection.name + PATH_SUFFIX,
                            PATH_TRAIT)
        # Add input fields
        for field_collection in self.field_collections:
            self._add_trait(self.inputs,
                            field_collection.name + FIELD_SUFFIX,
                            self.field_trait(field_collection))
        self._prov = provenance
        self._frequencies = frequencies

    def _list_outputs(self):
        outputs = self.output_spec().get()
        # Connect iterables (i.e. subject_id and visit_id)
        outputs['subject_id'] = self.inputs.subject_id
        outputs['visit_id'] = self.inputs.visit_id
        subject_id = (self.inputs.subject_id
                      if isdefined(self.inputs.subject_id) else None)
        visit_id = (self.inputs.visit_id
                    if isdefined(self.inputs.visit_id) else None)
        out_files = []
        out_fields = []
        missing_inputs = []
        # Collate frequency-specific input checksums into a single dictionary
        input_checksums = {}
        for freq in self._frequencies:
            input_checksums.update(
                getattr(self.inputs, '{}_checksums'.format(freq)))
        output_checksums = {}
        with ExitStack() as stack:
            # Connect to set of repositories that the collections come from
            for repository in self.repositories:
                stack.enter_context(repository)
            for fileset_collection in self.fileset_collections:
                fileset = fileset_collection.item(
                    subject_id,
                    visit_id)
                path = getattr(self.inputs,
                               fileset_collection.name + PATH_SUFFIX)
                if not isdefined(path):
                    missing_inputs.append(fileset.name)
                    continue  # skip the upload for this file
                fileset.path = path
                fileset.put()
                output_checksums[fileset.name] = fileset.checksums
            for field_collection in self.field_collections:
                field = field_collection.item(
                    subject_id,
                    visit_id)
                value = getattr(self.inputs,
                                field_collection.name + FIELD_SUFFIX)
                if not isdefined(value):
                    missing_inputs.append(field.name)
                    continue  # skip the upload for this file
                field.value = value
                field.put()
                out_fields.append((field.name, value))
                output_checksums[field.name] = field.value
            prov_record = self._prov.record(input_checksums, output_checksums,
                                            subject_id, visit_id)
            for repository in self.repositories:
                repository.put_provenance(prov_record)
        if missing_inputs:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the filesets that were created
            logger.warning(
                "Missing inputs '{}' in RepositorySink".format(
                    "', '".join(missing_inputs)))
        # Return cache file paths
        outputs['files'] = out_files
        outputs['fields'] = out_fields
        outputs['combined'] = out_files + out_fields
        return outputs
