from abc import ABCMeta, abstractmethod
from nianalysis.nodes import Node
from nianalysis.exceptions import (
    NiAnalysisRequirementVersionException,
    NiAnalysisModulesNotInstalledException,
    NiAnalysisUsageError)
from nianalysis.requirements import Requirement
import logging


logger = logging.getLogger('NiAnalysis')


class DataFormat(object):
    """
    Defines a format for a dataset (e.g. DICOM, NIfTI, Matlab file)

    Parameters
    ----------
    name : str
        A name for the data format
    extension : str
        The extension of the format
    description : str
        A description of what the format is and ideally a link to its
        documentation
    directory : bool
        Whether the format is a directory or a file
    within_dir_exts : List[str]
        A list of extensions that are found within the top level of
        the directory (for directory formats). Used to identify
        formats from paths.
    """

    def __init__(self, name, extension, description='',
                 directory=False, within_dir_exts=None):
        self._name = name
        self._extension = extension
        self._description = description
        self._directory = directory
        if within_dir_exts is not None:
            if not directory:
                raise NiAnalysisUsageError(
                    "'within_dir_exts' keyword arg is only valid "
                    "for directory data formats, not '{}'".format(name))
            within_dir_exts = frozenset(within_dir_exts)
        self._within_dir_exts = within_dir_exts

    def __eq__(self, other):
        try:
            return (
                self._name == other._name and
                self._extension == other._extension and
                self._description == other._description and
                self._directory == other._directory and
                self._within_dir_exts ==
                other._within_dir_exts)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("DataFormat(name='{}', extension='{}', directory={}{})"
                .format(self.name, self.extension, self.directory,
                        ('within_dir_extension={}'.format(
                            self.within_dir_exts)
                         if self.directory else '')))

    def __str__(self):
        return self.name

    @property
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension

    @property
    def ext_str(self):
        return self.extension if self.extension is not None else ''

    @property
    def description(self):
        return self._description

    @property
    def directory(self):
        return self._directory

    @property
    def within_dir_exts(self):
        return self._within_dir_exts

    @property
    def xnat_resource_name(self):
        return self.name.upper()


class Converter(object):
    """
    Base class for all NiAnalysis data format converters
    """

    __metaclass__ = ABCMeta

    def convert(self, workflow, source, dataset, dataset_name, node_name,
                output_format):
        """
        Inserts a format converter node into a complete workflow.

        Parameters
        ----------
        workflow : nipype.Workflow
            The workflow to add the converter node to
        source : nipype.Node
            The source node to draw the data from
        dataset : Dataset
            The dataset to convert
        node_name : str
            (Unique) name for the conversion node
        output_format : DataFormat
            The format to convert the node to
        """
        convert_node, in_field, out_field = self._get_convert_node(
            node_name, dataset.format, output_format)
        workflow.connect(
            source, dataset_name, convert_node, in_field)
        return convert_node, out_field

    @abstractmethod
    def _get_convert_node(self):
        pass

    @abstractmethod
    def input_formats(self):
        "Lists all data formats that the converter tool can read"
        pass

    @abstractmethod
    def output_formats(self):
        "Lists all data formats that the converter tool can write"
        pass

    @property
    def is_available(self):
        """
        Check to see if the required modules to run the conversion are
        available. Defaults to True if modules are not used on the system.
        """
        try:
            available_modules = Node.available_modules()
        except NiAnalysisModulesNotInstalledException:
            # Assume that it is installed but not as a module
            return True
        try:
            for possible_reqs in self.requirements:
                Requirement.best_requirement(possible_reqs, available_modules)
            return True
        except NiAnalysisRequirementVersionException:
            return False
