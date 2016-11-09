from abc import ABCMeta, abstractmethod
from nipype.pipeline import engine as pe
from nipype.interfaces.io import IOBase, add_traits
from nipype.interfaces.base import (
    DynamicTraitedSpec, traits, TraitedSpec, BaseInterfaceInputSpec,
    Undefined, isdefined)
from nianalysis.base import Scan
from nianalysis.exceptions import NiAnalysisError


INPUT_OUTPUT_SUFFIX = '_scan'


class Project(object):

    def __init__(self, project_id, subjects, scans):
        self._id = project_id
        self._subjects = subjects
        self._scans = scans

    @property
    def id(self):
        return self._id

    @property
    def subjects(self):
        return iter(self._subjects)

    @property
    def scans(self):
        return self._scans

    def __eq__(self, other):
        if not isinstance(other, Project):
            return False
        return (self._id == other._id and
                self._sessions == other._sessions)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Subject(id={}, num_sessions={})".format(self._id,
                                                        len(self._sessions))

    def __hash__(self):
        return hash(self._id)


class Subject(object):
    """
    Holds a subject id and a list of sessions
    """

    def __init__(self, subject_id, sessions, scans):
        self._id = subject_id
        self._sessions = sessions
        self._scans = scans
        for session in sessions:
            session.subject = self

    @property
    def id(self):
        return self._id

    @property
    def sessions(self):
        return iter(self._sessions)

    @property
    def scans(self):
        return self._scans

    def __eq__(self, other):
        if not isinstance(other, Subject):
            return False
        return (self._id == other._id and
                self._sessions == other._sessions)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Subject(id={}, num_sessions={})".format(self._id,
                                                        len(self._sessions))

    def __hash__(self):
        return hash(self._id)


class Session(object):
    """
    Holds the session id and the list of scans loaded from it
    """

    def __init__(self, session_id, scans, processed=None):
        self._id = session_id
        self._scans = scans
        self._subject = None
        self._processed = processed

    @property
    def id(self):
        return self._id

    @property
    def subject(self):
        return self._subject

    @subject.setter
    def subject(self, subject):
        self._subject = subject

    @property
    def processed(self):
        return self._processed

    @property
    def scans(self):
        return iter(self._scans)

    def __eq__(self, other):
        if not isinstance(other, Session):
            return False
        return (self._id == other._id and
                self._subject_id == other._subject_id and
                self._scans == other._scans)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "Session(id='{}', num_scans={})".format(self._id,
                                                       len(self._scans))

    def __hash__(self):
        return hash(self._id)


class Archive(object):
    """
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def source(self, project_id, input_scans, name=None):
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
            datasets to extract from the archive system for each session
        """
        if name is None:
            name = "{}_source".format(self.type)
        source = pe.Node(self.Source(), name=name)
        source.inputs.project_id = str(project_id)
        source.inputs.datasets = [s.to_tuple() for s in input_scans]
        return source

    @abstractmethod
    def sink(self, project_id, output_scans, multiplicity='per_session',
             name=None):
        """
        Returns a NiPype node that puts the output data back to the archive
        system. The input spec of the node's interface should inherit from
        ArchiveSinkInputSpec

        Parameters
        ----------
        project_id : str
            The ID of the project to return the sessions for

        """
        if multiplicity.startswith('per_session'):
            sink_class = self.Sink
        elif multiplicity.startswith('per_subject'):
            sink_class = self.SubjectSink
        elif multiplicity.startswith('per_project'):
            sink_class = self.ProjectSink
        else:
            raise NiAnalysisError(
                "Unrecognised multiplicity '{}' can be one of '{}'"
                .format(multiplicity, "', '".join(Scan.MULTIPLICITY_OPTIONS)))
        if name is None:
            name = "{}_{}_sink".format(self.type, multiplicity)
        output_scans = list(output_scans)  # Ensure iterators aren't exhausted
        sink = pe.Node(sink_class(output_scans), name=name)
        sink.inputs.project_id = str(project_id)
        sink.inputs.datasets = [s.to_tuple() for s in output_scans]
        return sink

    @abstractmethod
    def project(self, project_id, subject_ids=None, session_ids=None):
        """
        Returns a nianalysis.archive.Project object for the given project id,
        which holds information on all available subjects, sessions and scans
        in the project.

        Parameters
        ----------
        project_id : str
            The ID of the project to return the sessions for
        subject_ids : list(str)
            List of subject ids to filter the returned subjects. If None all
            subjects will be returned.
        session_ids : list(str)
            List of session ids to filter the returned sessions. If None all
            sessions will be returned
        """


class ArchiveSourceInputSpec(TraitedSpec):
    """
    Base class for archive source input specifications. Provides a common
    interface for 'run_pipeline' when using the archive source to extract
    acquired and preprocessed datasets from the archive system
    """
    project_id = traits.Str(  # @UndefinedVariable
        mandatory=True,
        desc='The project ID')
    subject_id = traits.Str(mandatory=True, desc="The subject ID")
    session_id = traits.Str(mandatory=True, usedefult=True,  # @UndefinedVariable @IgnorePep8
                            desc="The session or processed group ID")
    datasets = traits.List(
        Scan.traits_spec(),
        desc="Names of all datasets that comprise the (sub)project")


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
                                 for scan in self.inputs.datasets])


class BaseArchiveSinkInputSpec(DynamicTraitedSpec, BaseInterfaceInputSpec):
    """
    Base class for archive sink input specifications. Provides a common
    interface for 'run_pipeline' when using the archive save
    processed datasets in the archive system
    """
    project_id = traits.Str(  # @UndefinedVariable
        mandatory=True,
        desc='The project ID')  # @UndefinedVariable @IgnorePep8

    name = traits.Str(  # @UndefinedVariable @IgnorePep8
        mandatory=True, desc=("The name of the processed data group, e.g. "
                              "'tractography'"))
    description = traits.Str(mandatory=True,  # @UndefinedVariable
                             desc="Description of the study")
    datasets = traits.List(
        Scan.traits_spec(),
        desc="Names of all datasets that comprise the (sub)project")
    # TODO: Not implemented yet
    overwrite = traits.Bool(  # @UndefinedVariable
        False, mandatory=True, usedefault=True,
        desc=("Whether or not to overwrite previously created sessions of the "
              "same name"))

    def __setattr__(self, name, val):
        if isdefined(self.datasets) and not hasattr(self, name):
            accepted = [s[0] + ArchiveSink.INPUT_SUFFIX for s in self.datasets]
            try:
                assert name in accepted, (
                    "'{}' is not a valid input filename for '{}' archive sink "
                    "(accepts '{}')".format(name, self.name,
                                            "', '".join(accepted)))
            except:
                raise
        super(BaseArchiveSinkInputSpec, self).__setattr__(name, val)


class ArchiveSinkInputSpec(BaseArchiveSinkInputSpec):

    subject_id = traits.Str(mandatory=True, desc="The subject ID"),  # @UndefinedVariable @IgnorePep8
    session_id = traits.Str(mandatory=False,  # @UndefinedVariable @IgnorePep8
                            desc="The session or processed group ID")


class ArchiveSubjectSinkInputSpec(BaseArchiveSinkInputSpec):

    subject_id = traits.Str(mandatory=True, desc="The subject ID")  # @UndefinedVariable @IgnorePep8


class ArchiveProjectSinkInputSpec(BaseArchiveSinkInputSpec):
    pass


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
