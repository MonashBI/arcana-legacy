import os.path
from .base import Archive
from nipype.pipeline import engine as pe
from nipype.interfaces.io import DataGrabber


class LocalFileSystem(Archive):
    """
    Abstract base class for all Archive systems, DaRIS, XNAT and local file
    system. Sets out the interface that all Archive classes should implement.
    """

    def __init__(self, path):
        self._path = path

    def source(self, project_id, input_files):
        source = pe.Node(
            DataGrabber(infields=['subject_id', 'study_id'],
                        outfields=[f.name for f in input_files]),
            name="local_source")
        source.inputs.base_directory = os.path.join(self._path,
                                                    str(project_id))
        source.inputs.template = '*'
        field_template = {}
        template_args = {}
        for input_file in input_files:
            field_template[input_file.name] = '%s/%d/{}'.format(
                input_file.filename())
            template_args[input_file.name] = [['subject_id', 'study_id']]
        source.inputs.field_template = field_template
        source.inputs.template_args = template_args

    def sink(self, project_id):
        raise NotImplementedError

    def all_sessions(self, project_id, study_id=None):
        raise NotImplementedError

    def sessions_with_dataset(self, file_, project_id, sessions=None):
        raise NotImplementedError
