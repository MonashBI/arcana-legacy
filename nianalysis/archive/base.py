from abc import ABCMeta, abstractmethod
from nipype.pipeline import engine as pe
from nipype.interfaces.io import IOBase, add_traits
from nipype.interfaces.base import (
    DynamicTraitedSpec, traits, TraitedSpec, BaseInterfaceInputSpec,
    Undefined)
from nianalysis.base import Scan


INPUT_OUTPUT_SUFFIX = '_scan'


class Session(object):
    """
    A small wrapper class used to define the subject_id and study_id
    """

    def __init__(self, subject_id, study_id='1'):
        if isinstance(subject_id, self.__class__):
            # If subject_id is actually another Session just copy values
            self._subject_id = subject_id.subject_id
            self._study_id = subject_id.study_id
        else:
            self._subject_id = str(subject_id)
            self._study_id = str(study_id)

    def __eq__(self, other):
        return (self.subject_id == other.subject_id and
                self.study_id == other.study_id)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Session(subject_id='{}', session_id='{}')".format(
            self.subject_id, self.study_id)

    def __hash__(self):
        return hash((self.subject_id, self.study_id))

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def study_id(self):
        return self._study_id


class Archive(object):
    """
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def source(self, project_id, input_scans):
        """
        Returns a NiPype node that gets the input data from the archive
        system. The input spec of the node's interface should inherit from
        ArchiveSourceInputSpec

        Parameters
        ----------
        project_id : str
            The ID of the project to return the sessions for
        input_files : List[BaseFile]
            An iterable of nianalysis.BaseFile objects, which specify the
            files to extract from the archive system for each session
        """
        source = pe.Node(self.Source(), name="{}_source".format(self.type))
        source.inputs.project_id = str(project_id)
        source.inputs.files = [s.to_tuple() for s in input_scans]
        return source

    @abstractmethod
    def sink(self, project_id, output_scans):
        """
        Returns a NiPype node that puts the output data back to the archive
        system. The input spec of the node's interface should inherit from
        ArchiveSinkInputSpec

        Parameters
        ----------
        project_id : str
            The ID of the project to return the sessions for

        """
        sink = pe.Node(self.Sink(output_scans),
                       name="{}_sink".format(self.type))
        sink.inputs.project_id = str(project_id)
        sink.inputs.files = [s.to_tuple() for s in output_scans]
        return sink

    @abstractmethod
    def all_sessions(self, project_id, study_id=None):
        """
        Returns a nianalysis.Session object for each session acquired
        for the project.

        Parameters
        ----------
        project_id : str
            The ID of the project to return the sessions for
        study_id : str
            The ID of the study to return for each subject. If None then all
            studies are return for each subject.
        """

    @abstractmethod
    def sessions_with_file(self, scan, project_id, sessions=None):
        """
        Returns all the sessions (nianalysis.Session) in the given project
        that contain the given file

        Parameters
        ----------
        scan : nianalysis.Scan
            A Scan object which all sessions will be checked against to see
            whether they contain it
        project_id : str
            The ID of the project to return the sessions for
        sessions : List[Session]
            The list of sessions to check. If None then all sessions are
            checked for the given project
        """


class ArchiveSourceInputSpec(TraitedSpec):
    """
    Base class for archive source input specifications. Provides a common
    interface for 'run_pipeline' when using the archive source to extract
    acquired and preprocessed files from the archive system
    """
    project_id = traits.Str(  # @UndefinedVariable
        mandatory=True,
        desc='The project ID')
    session = traits.Tuple(  # @UndefinedVariable
        traits.Str(  # @UndefinedVariable
            mandatory=True,
            desc="The subject ID"),
        traits.Str(1, mandatory=True, usedefult=True,  # @UndefinedVariable @IgnorePep8
                   desc="The session or processed group ID"),
        mandatory=True, desc="The subjec/session pair to retrieve")
    files = traits.List(
        Scan.traits_spec(),
        desc="Names of all files that comprise the complete file")


class ArchiveSource(IOBase):

    __metaclass__ = ABCMeta

    output_spec = DynamicTraitedSpec
    _always_run = True

    OUTPUT_SUFFIX = '_fname'

    def __init__(self, infields=None, outfields=None, **kwargs):
        """
        Parameters
        ----------
        infields : list of str
            Indicates the input fields to be dynamically created

        outfields: list of str
            Indicates output fields to be dynamically created

        See class examples for usage

        """
        if not outfields:
            outfields = ['outfiles']
        super(ArchiveSource, self).__init__(**kwargs)
        undefined_traits = {}
        # used for mandatory inputs check
        self._infields = infields
        self._outfields = outfields
        if infields:
            for key in infields:
                self.inputs.add_trait(key, traits.Any)  # @UndefinedVariable
                undefined_traits[key] = Undefined

    @abstractmethod
    def _list_outputs(self):
        pass

    def _add_output_traits(self, base):
        return add_traits(base, [scan[0] + self.OUTPUT_SUFFIX
                                 for scan in self.inputs.files])


class ArchiveSinkInputSpec(DynamicTraitedSpec, BaseInterfaceInputSpec):
    """
    Base class for archive sink input specifications. Provides a common
    interface for 'run_pipeline' when using the archive save
    processed files in the archive system
    """
    project_id = traits.Str(  # @UndefinedVariable
        mandatory=True,
        desc='The project ID')  # @UndefinedVariable @IgnorePep8
    session = traits.Tuple(  # @UndefinedVariable
        traits.Str(  # @UndefinedVariable
            mandatory=True,
            desc="The subject ID"),  # @UndefinedVariable @IgnorePep8
        traits.Str(mandatory=False,  # @UndefinedVariable @IgnorePep8
                   desc="The session or processed group ID"))
    name = traits.Str(  # @UndefinedVariable @IgnorePep8
        mandatory=True, desc=("The name of the processed data group, e.g. "
                              "'tractography'"))
    description = traits.Str(mandatory=True,  # @UndefinedVariable
                             desc="Description of the study")
    files = traits.List(
        Scan.traits_spec(),
        desc="Names of all files that comprise the complete file")
    # TODO: Not implemented yet
    overwrite = traits.Bool(  # @UndefinedVariable
        False, mandatory=True, usedefault=True,
        desc=("Whether or not to overwrite previously created studies of the "
              "same name"))


class ArchiveSinkOutputSpec(TraitedSpec):

    out_files = traits.Any(desc='datasink output')  # @UndefinedVariable


class ArchiveSink(IOBase):

    __metaclass__ = ABCMeta

    input_spec = ArchiveSinkInputSpec
    output_spec = ArchiveSinkOutputSpec

    INPUT_SUFFIX = '_fname'

    def __init__(self, output_scans, **kwargs):
        """
        Parameters
        ----------
        infields : list of str
            Indicates the input fields to be dynamically created

        outfields: list of str
            Indicates output fields to be dynamically created

        See class examples for usage

        """
        super(ArchiveSink, self).__init__(**kwargs)
        # used for mandatory inputs check
        self._infields = None
        self._outfields = None
        add_traits(self.inputs, [s.name + self.INPUT_SUFFIX
                                 for s in output_scans])

    @abstractmethod
    def _list_outputs(self):
        pass
