from nipype.interfaces.fsl.maths import BinaryMaths
from nianalysis.interfaces.utils import Merge
from nianalysis.dataset import DatasetSpec, Dataset
from nianalysis.data_formats import nifti_gz_format
from nianalysis.study.base import Study, set_dataset_specs
from nianalysis.testing import BaseTestCase
from nianalysis.requirements import fsl5_req
import logging

logger = logging.getLogger('NiAnalysis')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class RequirementsStudy(Study):

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('ones', nifti_gz_format)],
            outputs=[DatasetSpec('twos', nifti_gz_format)],
            description=("A pipeline that tests loading of requirements"),
            default_options={},
            version=1,
            citations=[],)
        # Convert from DICOM to NIfTI.gz format on input
        merge = pipeline.create_node(Merge(2), 'merge')
        maths = pipeline.create_node(
            BinaryMaths(), "maths", required=[fsl5_req])
        pipeline.connect_input('ones', merge, 'in1')
        pipeline.connect_input('ones', merge, 'in2')
        pipeline.connect(merge, 'out', maths, 'in_file')
        pipeline.connect_output('twos', maths, 'out_file')
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('ones', nifti_gz_format),
        DatasetSpec('twos', nifti_gz_format, pipeline))


class TestModuleLoad(BaseTestCase):

    def test_pipeline_prerequisites(self):
        study = self.create_study(
            RequirementsStudy, 'requirements',
            {'ones': Dataset('ones', nifti_gz_format)})
        study.pipeline().run(work_dir=self.work_dir)
        self.assertDatasetCreated('twos.nii.gz', study.name)
