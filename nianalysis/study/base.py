from abc import ABCMeta
from logging import getLogger
from nianalysis.exceptions import (
    NiAnalysisMissingDatasetError, NiAnalysisNameError,
    NiAnalysisUsageError)
from nianalysis.pipeline import Pipeline
from nianalysis.dataset import BaseDatum


logger = getLogger('NiAnalysis')


class Study(object):
    """
    Abstract base study class from which all study derive.

    Parameters
    ----------
    name : str
        The name of the study.
    project_id: str
        The ID of the archive project from which to access the data from. For
        DaRIS it is the project id minus the proceeding 1008.2. For XNAT it
        will be the project code. For local archives name of the directory.
    archive : Archive
        An Archive object that provides access to a DaRIS, XNAT or local file
        system
    inputs : Dict[str, base.Dataset]
        A dict containing the a mapping between names of study data_specs
        and existing datasets (typically primary from the scanner but can
        also be replacements for generated data_specs)


    Required Sub-Class attributes
    -----------------------------
    data_specs : dict[str, DatasetSpec]
        Subclasses of Study need to have a 'data_specs' class attribute,
        which is a dictionary that maps the names of datasets that are used and
        generated by the study to DatasetSpec objects. The function
        `set_specs` is a convenient method to generate such a
        dictionary.
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, project_id, archive, inputs, check_inputs=True):
        self._name = name
        self._project_id = project_id
        self._inputs = {}
        # Add each "input dataset" checking to see whether the given
        # dataset_spec name is valid for the study type
        for name, inpt in inputs.iteritems():
            if name not in self._data_specs:
                raise NiAnalysisNameError(
                    "Input dataset data_spec name '{}' doesn't match any "
                    "data_specs in {} studies".format(
                        name, self.__class__.__name__))
            self._inputs[name] = inpt
        # Emit a warning if an acquired dataset has not been provided for
        # an "acquired dataset"
        if check_inputs:
            for spec in self.acquired_data_specs():
                if spec.name not in self._inputs:
                    logger.warning(
                        "'{}' acquired dataset was not specified in {} "
                        "'{}' (provided '{}'). Pipelines depending on this "
                        "dataset will not run".format(
                            spec.name, self.__class__.__name__, self.name,
                            "', '".join(self._inputs)))
        # TODO: Check that every session has the primary datasets
        self._archive = archive

    def __repr__(self):
        """String representation of the study"""
        return "{}(name='{}')".format(self.__class__.__name__, self.name)

    def dataset(self, name):
        """
        Returns either the dataset/field that has been passed to the study
        __init__ matching the dataset/field name provided or the processed
        dataset that is to be generated using the pipeline associated with the
        generated data_spec

        Parameters
        ----------
        name : Str
            Name of the dataset_spec to the find the corresponding primary
            dataset or processed dataset to be generated
        """
        if isinstance(name, BaseDatum):
            name = name.name
        try:
            dataset = self._inputs[name]
        except KeyError:
            try:
                dataset = self._data_specs[name].apply_prefix(self.prefix)
            except KeyError:
                raise NiAnalysisNameError(
                    "'{}' is not a recognised dataset_spec name for {} "
                    "studies."
                    .format(name, self.__class__.__name__))
            if not dataset.processed:
                raise NiAnalysisMissingDatasetError(
                    "Acquired (i.e. non-generated) dataset '{}' "
                    "was not supplied when the study '{}' was initiated"
                    .format(name, self.name))
        return dataset

    @property
    def inputs(self):
        return self._inputs.values()

    @property
    def prefix(self):
        """The study name as a prefix for dataset names"""
        return self.name + '_'

    @property
    def project_id(self):
        """Accessor for the project id"""
        return self._project_id

    @property
    def name(self):
        """Accessor for the unique study name"""
        return self._name

    @property
    def archive(self):
        """Accessor for the archive member (e.g. Daris, XNAT, MyTardis)"""
        return self._archive

    def create_pipeline(self, *args, **options):
        """
        Creates a Pipeline object, passing the study (self) as the first
        argument
        """
        return Pipeline(self, *args, **options)

    @classmethod
    def data_spec(cls, name):
        """
        Return the dataset_spec, i.e. the template of the dataset expected to
        be supplied or generated corresponding to the dataset_spec name.

        Parameters
        ----------
        name : Str
            Name of the dataset_spec to return
        """
        if isinstance(name, BaseDatum):
            name = name.name
        try:
            return cls._data_specs[name]
        except KeyError:
            raise NiAnalysisNameError(
                "No dataset spec named '{}' in {} (available: "
                "'{}')".format(name, cls.__name__,
                               "', '".join(cls._data_specs.keys())))

    @classmethod
    def data_spec_names(cls):
        """Lists the names of all data_specs defined in the study"""
        return cls._data_specs.iterkeys()

    @classmethod
    def data_specs(cls):
        """Lists all data_specs defined in the study class"""
        return cls._data_specs.itervalues()

    @classmethod
    def acquired_data_specs(cls):
        """
        Lists all data_specs defined in the study class that are
        provided as inputs to the study
        """
        return (c for c in cls.data_specs() if not c.processed)

    @classmethod
    def generated_data_specs(cls):
        """
        Lists all data_specs defined in the study class that are typically
        generated from other data_specs (but can be overridden by input
        datasets)
        """
        return (c for c in cls.data_specs() if c.processed)

    @classmethod
    def generated_data_spec_names(cls):
        """Lists the names of generated data_specs defined in the study"""
        return (c.name for c in cls.generated_data_specs())

    @classmethod
    def acquired_data_spec_names(cls):
        "Lists the names of acquired data_specs defined in the study"
        return (c.name for c in cls.acquired_data_specs())


def set_specs(*comps, **kwargs):
    """
    Used to set the dataset specs in every Study class.

    Parameters
    ----------
    specs : list(DatasetSpec)
        List of dataset specs to set into the class
    inherit_from : list(
        The dataset specs from which to inherit *before* the explicitly added
        specs. Used to include dataset specs from base classes and then
        selectively override them.
    """
    dct = {}
    for comp in comps:
        if comp.name in dct:
            assert False, ("Multiple values for '{}' found in component list"
                           .format(comp.name))
        dct[comp.name] = comp
    if 'inherit_from' in kwargs:
        combined = set_specs(*set(kwargs['inherit_from']))
        # Allow the current components to override the inherited ones
        combined.update(dct)
        dct = combined
    return dct
