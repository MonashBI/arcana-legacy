from builtins import zip
from itertools import chain
from nipype.interfaces.base import TraitedSpec, traits, BaseInterface
from arcana.exception import ArcanaError


class InputSubjectsInputSpec(TraitedSpec):

    subject_id = traits.Str(mandatory=True, desc=("The subject ID"))
    prereq_reports = traits.List(
        traits.Tuple(traits.Str, traits.Str), value=[],
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

    visit_id = traits.Str(mandatory=True, desc=("The visit ID"))
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
        outputs['visit_id'] = self.inputs.visit_id
        outputs['subject_id'] = self.inputs.subject_id
        return outputs


class SessionRepositoryrtInputSpec(TraitedSpec):

    sessions = traits.List(traits.Str)
    subjects = traits.List(traits.Str)


class SessionRepositoryrtOutputSpec(TraitedSpec):

    subject_session_pairs = traits.List(traits.Tuple(traits.Str, traits.Str))


class SessionRepositoryrt(BaseInterface):
    """
    Basically an IndentityInterface for joining over sessions
    """

    input_spec = SessionRepositoryrtInputSpec
    output_spec = SessionRepositoryrtOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_session_pairs'] = list(zip(self.inputs.subjects,
                                                    self.inputs.sessions))
        return outputs


class SubjectRepositoryrtSpec(TraitedSpec):

    subjects = traits.List(traits.Str)


class SubjectRepositoryrt(BaseInterface):
    """
    Basically an IndentityInterface for joining over subjects
    """

    input_spec = SubjectRepositoryrtSpec
    output_spec = SubjectRepositoryrtSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subjects'] = self.inputs.subjects
        return outputs


class VisitRepositoryrtSpec(TraitedSpec):

    sessions = traits.List(traits.Str)


class VisitRepositoryrt(BaseInterface):
    """
    Basically an IndentityInterface for joining over sessions
    """

    input_spec = VisitRepositoryrtSpec
    output_spec = VisitRepositoryrtSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['sessions'] = self.inputs.sessions
        return outputs


class SubjectSessionRepositoryrtInputSpec(TraitedSpec):

    subject_session_pairs = traits.List(
        traits.List(traits.Tuple(traits.Str, traits.Str)))


class SubjectSessionRepositoryrtOutputSpec(TraitedSpec):

    subject_session_pairs = traits.List(traits.Tuple(traits.Str, traits.Str))


class SubjectSessionRepositoryrt(BaseInterface):
    """
    Basically an IndentityInterface for joining over subject-session pairs
    """

    input_spec = SubjectSessionRepositoryrtInputSpec
    output_spec = SubjectSessionRepositoryrtOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_session_pairs'] = list(
            chain(*self.inputs.subject_session_pairs))
        return outputs


class PipelineRepositoryrtInputSpec(TraitedSpec):
    subject_session_pairs = traits.List(traits.Tuple(
        traits.Str, traits.Str),
        desc="Subject & session pairs from per-session sink")
    subjects = traits.List(traits.Str,
                           desc="Subjects from per-subject sink")
    visits = traits.List(traits.Str,
                             desc="Visits from per_visit sink")
    project = traits.Str(desc="Project ID from per-project sink")


class PipelineRepositoryrtOutputSpec(TraitedSpec):
    subject_session_pairs = traits.List(traits.Tuple(
        traits.Str, traits.Str),
        desc="Session & subject pairs from per-session sink")
    subjects = traits.List(traits.Str, desc="Subjects from per-subject sink")
    project = traits.Str(desc="Project ID from per-project sink")


class PipelineRepositoryrt(BaseInterface):

    input_spec = PipelineRepositoryrtInputSpec
    output_spec = PipelineRepositoryrtOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        outputs['subject_session_pairs'] = self.inputs.subject_session_pairs
        outputs['subjects'] = self.inputs.subjects
        outputs['project'] = self.inputs.project
        return outputs


class SelectSessionInputSpec(TraitedSpec):
    subject_id = traits.Str(mandatory=True, desc="The subject ID to select")
    visit_id = traits.Str(mandatory=True, desc="The visit ID to select")
    subject_ids = traits.List(
        traits.Str(), mandatory=True,
        desc="The subject IDs to select the subject_id from")
    visit_ids = traits.List(
        traits.Str(), mandatory=True,
        desc="The visit IDs to select the visit_id from")
    items = traits.List(
        traits.Any(),
        mandatory=True,
        desc=("The items from which to select the one corresponding to the "
              "session from"))


class SelectSessionOutputSpec(TraitedSpec):
    item = traits.Any(desc="The selected item")


class SelectSession(BaseInterface):

    input_spec = SelectSessionInputSpec
    output_spec = SelectSessionOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = {}
        session_id = (self.inputs.subject_id, self.inputs.visit_id)
        session_ids = list(zip(self.inputs.subject_ids, self.inputs.visit_ids))
        if session_ids.count(session_id) != 1:
            raise ArcanaError(
                "More than one indices matched {} in subjects and visits list "
                "({})".format(session_id, session_ids))
        index = session_ids.index(session_id)
        outputs['item'] = self.inputs.items[index]
        return outputs
