from itertools import chain
from nipype.interfaces.base import TraitedSpec, traits, BaseInterface


class InputSubjectsInputSpec(TraitedSpec):

    subject_id = traits.Str(mandatory=True, desc=("The subject ID"))
    prereq_reports = traits.List(
        traits.Tuple(traits.Str, traits.Str),
        desc=("A list of session reports from all prerequisite pipelines"))


class InputSubjectsOutputSpec(TraitedSpec):

    subject_id = traits.Str(mandatory=True, desc=("The subject ID"))


class InputSubjects(BaseInterface):
    """
    Basically an IndentityInterface for iterating over all subjects with an
    extra input to feed the output summaries of prerequisite pipelines into to
    make sure they are run before the current pipeline.
    """

    input_spec = InputSubjectsInputSpec
    output_spec = InputSubjectsOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_id'] = self.inputs.subject_id
        return outputs


class InputSessionsSpec(TraitedSpec):

    session_id = traits.Str(mandatory=True, desc=("The session ID"))
    subject_id = traits.Str(mandatory=True, desc=("The subject ID"))


class InputSessions(BaseInterface):
    """
    Basically an IndentityInterface for iterating over all session.
    """

    input_spec = InputSessionsSpec
    output_spec = InputSessionsSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['session_id'] = self.inputs.session_id
        outputs['subject_id'] = self.inputs.subject_id
        return outputs


class SessionReportInputSpec(TraitedSpec):

    sessions = traits.List(traits.Str)
    subjects = traits.List(traits.Str)


class SessionReportOutputSpec(TraitedSpec):

    subject_session_pairs = traits.List(traits.Tuple(traits.Str, traits.Str))


class SessionReport(BaseInterface):
    """
    Basically an IndentityInterface for joining over sessions
    """

    input_spec = SessionReportInputSpec
    output_spec = SessionReportOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_session_pairs'] = list(zip(self.inputs.subjects,
                                                    self.inputs.sessions))
        return outputs


class SubjectReportSpec(TraitedSpec):

    subjects = traits.List(traits.Str)


class SubjectReport(BaseInterface):
    """
    Basically an IndentityInterface for joining over subjects
    """

    input_spec = SubjectReportSpec
    output_spec = SubjectReportSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subjects'] = self.inputs.subjects
        return outputs


class SubjectSessionReportInputSpec(TraitedSpec):

    subject_session_pairs = traits.List(
        traits.List(traits.Tuple(traits.Str, traits.Str)))


class SubjectSessionReportOutputSpec(TraitedSpec):

    subject_session_pairs = traits.List(traits.Tuple(traits.Str, traits.Str))


class SubjectSessionReport(BaseInterface):
    """
    Basically an IndentityInterface for joining over subject-session pairs
    """

    input_spec = SubjectSessionReportInputSpec
    output_spec = SubjectSessionReportOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_session_pairs'] = list(
            chain(*self.inputs.subject_session_pairs))
        return outputs


class PipelineReportInputSpec(TraitedSpec):
    subject_session_pairs = traits.List(traits.Tuple(
        traits.Str, traits.Str),
        desc="Subject & session pairs from per-session sink")
    subjects = traits.List(traits.Str, desc="Subjects from per-subject sink")
    project = traits.Str(desc="Project ID from per-project sink")


class PipelineReportOutputSpec(TraitedSpec):
    subject_session_pairs = traits.List(traits.Tuple(
        traits.Str, traits.Str),
        desc="Session & subject pairs from per-session sink")
    subjects = traits.List(traits.Str, desc="Subjects from per-subject sink")
    project = traits.Str(desc="Project ID from per-project sink")


class PipelineReport(BaseInterface):

    input_spec = PipelineReportInputSpec
    output_spec = PipelineReportOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_session_pairs'] = self.inputs.subject_session_pairs
        outputs['subjects'] = self.inputs.subjects
        outputs['project'] = self.inputs.project
        return outputs
