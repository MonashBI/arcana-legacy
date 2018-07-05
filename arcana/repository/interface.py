from nipype.interfaces.base import (
    traits, DynamicTraitedSpec, Undefined, File, Directory,
    BaseInterface, isdefined)
from arcana.dataset import (
    DatasetSpec, FieldSpec, BaseField, BaseDataset)
from arcana.utils import PATH_SUFFIX, FIELD_SUFFIX
import logging

logger = logging.getLogger('arcana')

PATH_TRAIT = traits.Either(File(exists=True), Directory(exists=True))
FIELD_TRAIT = traits.Either(traits.Int, traits.Float, traits.Str)
MULTIPLICITIES = ('per_session', 'per_subject', 'per_visit', 'per_project')


class BaseRepositoryNode(BaseInterface):
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
        super(BaseRepositoryNode, self).__init__()
        self._repositories = set(c.repository for c in collections
                                 if c.repository is not None)
        self._datasets = [c for c in collections
                          if isinstance(c, BaseDataset)]
        self._fields = [c for c in collections
                        if isinstance(c, BaseField)]

    def __eq__(self, other):
        try:
            return (self.datasets == other.datasets and
                    self.fields == other.fields)
        except AttributeError:
            return False

    def __repr__(self):
        return "{}(datasets={}, fields={})".format(
            type(self).__name__, self.datasets, self.fields)

    def __ne__(self, other):
        return not self == other

    def _run_interface(self, runtime, *args, **kwargs):  # @UnusedVariable
        return runtime

    @property
    def datasets(self):
        return self._datasets

    @property
    def fields(self):
        return self._fields

    @classmethod
    def _add_trait(cls, spec, name, trait_type):
        spec.add_trait(name, trait_type)
        spec.trait_set(trait_change_notify=False, **{name: Undefined})
        # Access the trait (not sure why but this is done in add_traits
        # so I have also done it here
        getattr(spec, name)


class RepositorySourceSpec(DynamicTraitedSpec):
    """
    Base class for repository sink and source input specifications.
    """
    subject_id = traits.Str(mandatory=True, desc="The subject ID")
    visit_id = traits.Str(mandatory=True, usedefult=True,
                          desc="The visit ID")


class RepositorySource(BaseRepositoryNode):
    """
    Parameters
    ----------
    datasets: list
        List of all datasets to be extracted from the repository
    fields: list
        List of all the fields that are to be extracted from the repository
    study_name: str
        Prefix prepended onto derived dataset "names"
    """

    input_spec = RepositorySourceSpec
    output_spec = RepositorySourceSpec
    _always_run = True

    def _outputs(self):
        outputs = super(RepositorySource, self)._outputs()
        # Add output datasets
        for dataset in self.datasets:
            self._add_trait(outputs, dataset.name + PATH_SUFFIX,
                            PATH_TRAIT)
        # Add output fields
        for field in self.fields:
            self._add_trait(outputs, field.name + FIELD_SUFFIX,
                            field.dtype)
        return outputs

    def _list_outputs(self):
        # Directory that holds session-specific
        outputs = {}
        # Source datasets
        with self._repositories:
            for dataset in self.datasets:
                dataset.get()
                outputs[dataset.name + PATH_SUFFIX] = dataset.path
            for field in self.fields:
                field.get()
                outputs[field.name + FIELD_SUFFIX] = field.value
        return outputs


class BaseRepositorySink(BaseRepositoryNode):

    def __init__(self, datasets, fields):
        super(BaseRepositorySink, self).__init__(datasets, fields)
        # Add input datasets
        for dataset in datasets:
            assert isinstance(dataset, DatasetSpec)
            self._add_trait(self.inputs, dataset.name + PATH_SUFFIX,
                            PATH_TRAIT)
        # Add input fields
        for field in fields:
            assert isinstance(field, FieldSpec)
            self._add_trait(self.inputs, field.name + FIELD_SUFFIX,
                            field.dtype)

    def _list_outputs(self):
        outputs = self.output_spec().get()
        # Connect iterables (i.e. subject_id and visit_id)
        for attr in self.iter_attr:
            outputs[attr] = getattr(self.inputs, attr)
        out_files = []
        out_fields = []
        missing_inputs = []
        with self._repositories:
            for dataset in self.datasets:
                path = getattr(self.inputs, dataset.name + PATH_SUFFIX)
                if not isdefined(path):
                    missing_inputs.append(dataset.name)
                    continue  # skip the upload for this file
                dataset.path = path
                dataset.put()
            for field in self.fields:
                value = getattr(self.inputs, field.name + FIELD_SUFFIX)
                if not isdefined(value):
                    missing_inputs.append(field.name)
                    continue  # skip the upload for this file
                field.value = value
                field.put()
                out_fields.append((field.name, value))
        if missing_inputs:
            # FIXME: Not sure if this should be an exception or not,
            #        indicates a problem but stopping now would throw
            #        away the datasets that were created
            logger.warning(
                "Missing inputs '{}' in RepositorySink".format(
                    "', '".join(missing_inputs)))
        # Return cache file paths
        outputs['out_files'] = out_files
        outputs['out_fields'] = out_fields
        return outputs


class BaseRepositorySinkSpec(DynamicTraitedSpec):
    pass


class RepositorySessionSinkInputSpec(BaseRepositorySinkSpec):

    subject_id = traits.Str(mandatory=True, desc="The subject ID"),
    visit_id = traits.Str(mandatory=False,
                            desc="The session or derived group ID")


class RepositorySubjectSinkInputSpec(BaseRepositorySinkSpec):

    subject_id = traits.Str(mandatory=True, desc="The subject ID")


class RepositoryVisitSinkInputSpec(BaseRepositorySinkSpec):

    visit_id = traits.Str(mandatory=True, desc="The visit ID")


class RepositoryProjectSinkInputSpec(BaseRepositorySinkSpec):
    pass


class BaseRepositorySinkOutputSpec(DynamicTraitedSpec):

    out_files = traits.List(PATH_TRAIT, desc='Output datasets')

    out_fields = traits.List(
        traits.Tuple(traits.Str, FIELD_TRAIT), desc='Output fields')


class RepositorySessionSinkOutputSpec(BaseRepositorySinkOutputSpec):

    subject_id = traits.Str(desc="The subject ID")
    visit_id = traits.Str(desc="The visit ID")


class RepositorySubjectSinkOutputSpec(BaseRepositorySinkOutputSpec):

    subject_id = traits.Str(desc="The subject ID")


class RepositoryVisitSinkOutputSpec(BaseRepositorySinkOutputSpec):

    visit_id = traits.Str(desc="The visit ID")


class RepositoryProjectSinkOutputSpec(BaseRepositorySinkOutputSpec):

    project_id = traits.Str(desc="The project ID")


class RepositorySessionSink(BaseRepositorySink):

    input_spec = RepositorySessionSinkInputSpec
    output_spec = RepositorySessionSinkOutputSpec

    frequency = 'per_session'
    iter_attr = ['subject_id', 'visit_id']


class RepositorySubjectSink(BaseRepositorySink):

    input_spec = RepositorySubjectSinkInputSpec
    output_spec = RepositorySubjectSinkOutputSpec

    frequency = 'per_subject'
    iter_attr = ['subject_id']


class RepositoryVisitSink(BaseRepositorySink):

    input_spec = RepositoryVisitSinkInputSpec
    output_spec = RepositoryVisitSinkOutputSpec

    frequency = 'per_visit'
    iter_attr = ['visit_id']


class RepositoryProjectSink(BaseRepositorySink):

    input_spec = RepositoryProjectSinkInputSpec
    output_spec = RepositoryProjectSinkOutputSpec

    frequency = 'per_project'
    iter_attr = []