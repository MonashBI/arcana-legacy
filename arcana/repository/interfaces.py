from arcana.utils import ExitStack
from nipype.interfaces.base import (
    traits, DynamicTraitedSpec, Undefined, File, Directory,
    BaseInterface, isdefined)
from itertools import chain
from copy import copy
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX, CHECKSUM_SUFFIX
from arcana.pipeline.provenance import Record
from arcana.exceptions import ArcanaError
import logging

logger = logging.getLogger('arcana')

PATH_TRAIT = traits.Either(File(exists=True), Directory(exists=True))
FIELD_TRAIT = traits.Either(traits.Int, traits.Float, traits.Str,
                            traits.List(traits.Int), traits.List(traits.Float),
                            traits.List(traits.Str))
CHECKSUM_TRAIT = traits.Dict(traits.Str(), traits.Str())
# Trait for checksums that may be joined over iterators
JOINED_CHECKSUM_TRAIT = traits.Either(
    CHECKSUM_TRAIT, traits.List(CHECKSUM_TRAIT),
    traits.List(traits.List(CHECKSUM_TRAIT)))


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
        # Protect against iterators
        collections = list(collections)
        # Check for consistent frequencies in collections
        frequencies = set(c.frequency for c in collections)
        if len(frequencies) > 1:
            raise ArcanaError(
                "Attempting to sink multiple frequencies across collections {}"
                .format(', '.join(str(c) for c in collections)))
        elif frequencies:
            # NB: Exclude very rare case where pipeline doesn't have inputs,
            #     would only really happen in unittests
            self._frequency = next(iter(frequencies))
        # Extract set of repositories used to source/sink from/to
        self.repositories = set(c.repository for c in collections
                                 if c.repository is not None)
        # Segregate into fileset and field collections
        self.fileset_collections = [c for c in collections if c.is_fileset]
        self.field_collections = [c for c in collections if c.is_field]

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

    @property
    def collections(self):
        return chain(self.fileset_collections, self.field_collections)

    @property
    def frequency(self):
        return self._frequency

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
    output_spec = BaseRepositorySpec
    _always_run = True

    def _outputs(self):
        outputs = super(RepositorySource, self)._outputs()
        # Add traits for filesets to source and their checksums
        for fileset_collection in self.fileset_collections:
            self._add_trait(outputs,
                            fileset_collection.name + PATH_SUFFIX, PATH_TRAIT)
            self._add_trait(outputs,
                            fileset_collection.name + CHECKSUM_SUFFIX,
                            CHECKSUM_TRAIT)
        # Add traits for fields to source
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
        with ExitStack() as stack:
            # Connect to set of repositories that the collections come from
            for repository in self.repositories:
                stack.enter_context(repository)
            for fileset_collection in self.fileset_collections:
                fileset = fileset_collection.item(subject_id, visit_id)
                fileset.get()
                outputs[fileset_collection.name + PATH_SUFFIX] = fileset.path
                outputs[fileset_collection.name +
                        CHECKSUM_SUFFIX] = fileset.checksums
            for field_collection in self.field_collections:
                field = field_collection.item(subject_id, visit_id)
                field.get()
                outputs[field_collection.name + FIELD_SUFFIX] = field.value
        return outputs


class RepositorySinkOutputSpec(DynamicTraitedSpec):

    checksums = traits.Either(
        traits.Dict, FIELD_TRAIT,
        desc=("Provenance information sinked with files and fields. Note that"
              "at this stage it is only used as something to connect to the "
              "\"deiterators\" and eventually the \"final\" node after the "
              "pipeline outputs have been sunk"))


class RepositorySink(BaseRepositoryInterface):
    """
    Interface used to sink derivatives into the output repository

    Parameters
    ----------
    collections : *Collection
        The collections of Field and Fileset objects to insert into the
        outputs repositor(y|ies)
    provenance : arcana.provenance.PipelineProvenance
        The pipeline provenance record (as opposed to the session or subject|
        visit|study-summary specific record that contains field values and
        file-set checksums for relevant inputs and outputs)
    """

    input_spec = BaseRepositorySpec
    output_spec = RepositorySinkOutputSpec

    def __init__(self, collections, provenance, pipeline_inputs):
        super(RepositorySink, self).__init__(collections)
        # Add traits for filesets to sink
        for fileset_collection in self.fileset_collections:
            self._add_trait(self.inputs,
                            fileset_collection.name + PATH_SUFFIX,
                            PATH_TRAIT)
        # Add traits for fields to sink
        for field_collection in self.field_collections:
            self._add_trait(self.inputs,
                            field_collection.name + FIELD_SUFFIX,
                            self.field_trait(field_collection))
        # Add traits for checksums/values of pipeline inputs
        pipeline_inputs = list(pipeline_inputs)
        for inpt in pipeline_inputs:
            if inpt.is_fileset:
                trait_t = JOINED_CHECKSUM_TRAIT
            else:
                trait_t = self.field_trait(inpt)
                trait_t = traits.Either(trait_t, traits.List(trait_t),
                                        traits.List(traits.List(trait_t)))
            self._add_trait(self.inputs, inpt.checksum_suffixed_name, trait_t)
        self._pipeline_input_filesets = [i.name for i in pipeline_inputs
                                         if i.is_fileset]
        self._pipeline_input_fields = [i.name for i in pipeline_inputs
                                       if i.is_field]
        self._prov = provenance
        self._pipeline_name = provenance['name']
        self._from_study = provenance['study']['name']

    def _list_outputs(self):
        outputs = self.output_spec().get()
        # Connect iterables (i.e. subject_id and visit_id)
        subject_id = (self.inputs.subject_id
                      if isdefined(self.inputs.subject_id) else None)
        visit_id = (self.inputs.visit_id
                    if isdefined(self.inputs.visit_id) else None)
        missing_inputs = []
        # Collate input checksums into a dictionary
        input_checksums = {n: getattr(self.inputs, n + CHECKSUM_SUFFIX)
                           for n in self._pipeline_input_filesets}
        input_checksums.update({n: getattr(self.inputs, n + FIELD_SUFFIX)
                                for n in self._pipeline_input_fields})
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
                try:
                    field.value = value
                except:
                    raise
                field.put()
                output_checksums[field.name] = field.value
            # Add input and output checksums to provenance record and sink to
            # all repositories that have received data (typically only one)
            prov = copy(self._prov)
            prov['inputs'] = input_checksums
            prov['outputs'] = output_checksums
            record = Record(self._pipeline_name, self.frequency, subject_id,
                            visit_id, self._from_study, prov)
            for repository in self.repositories:
                repository.put_record(record)
        if missing_inputs:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the filesets that were created
            logger.warning(
                "Missing inputs '{}' in RepositorySink".format(
                    "', '".join(missing_inputs)))
        # Return cache file paths
        outputs['checksums'] = output_checksums
        return outputs
