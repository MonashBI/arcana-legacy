from nianalysis.dataset import DatasetSpec, Dataset
from nianalysis.data_formats import (
    nifti_gz_format, mrtrix_format, dicom_format, directory_format, zip_format,
    nifti_format)
from nianalysis.requirements import mrtrix3_req
from nianalysis.study.base import Study, set_dataset_specs
from nianalysis.testing import BaseTestCase
from nipype.interfaces.utility import IdentityInterface
import logging

logger = logging.getLogger('NiAnalysis')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class ConversionStudy(Study):

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('mrtrix', mrtrix_format),
                    DatasetSpec('nifti_gz', nifti_gz_format),
                    DatasetSpec('dicom', nifti_gz_format),
                    DatasetSpec('directory', directory_format),
                    DatasetSpec('zip', directory_format)],
            outputs=[DatasetSpec('nifti_gz_from_dicom', nifti_gz_format),
                     DatasetSpec('mrtrix_from_nifti_gz', nifti_gz_format),
                     DatasetSpec('nifti_from_mrtrix', mrtrix_format),
                     DatasetSpec('directory_from_zip', directory_format),
                     DatasetSpec('zip_from_directory', directory_format)],
            description=("A pipeline that tests out various data format "
                         "conversions"),
            default_options={},
            version=1,
            citations=[],)
        # Convert from DICOM to NIfTI.gz format on input
        nifti_gz_from_dicom = pipeline.create_node(
            IdentityInterface(fields=['file']), "nifti_gz_from_dicom")
        pipeline.connect_input('dicom', nifti_gz_from_dicom,
                               'file')
        pipeline.connect_output('nifti_gz_from_dicom', nifti_gz_from_dicom,
                                'file')
        # Convert from NIfTI.gz to MRtrix format on output
        mrtrix_from_nifti_gz = pipeline.create_node(
            IdentityInterface(fields=['file']), name='mrtrix_from_nifti_gz')
        pipeline.connect_input('nifti_gz', mrtrix_from_nifti_gz,
                               'file')
        pipeline.connect_output('mrtrix_from_nifti_gz', mrtrix_from_nifti_gz,
                                'file')
        # Convert from MRtrix to NIfTI format on output
        nifti_from_mrtrix = pipeline.create_node(
            IdentityInterface(fields=['file']), 'nifti_from_mrtrix')
        pipeline.connect_input('mrtrix', nifti_from_mrtrix,
                               'file')
        pipeline.connect_output('nifti_from_mrtrix', nifti_from_mrtrix,
                                'file')
        # Convert from zip file to directory format on input
        directory_from_zip = pipeline.create_node(
            IdentityInterface(fields=['file']), 'directory_from_zip')
        pipeline.connect_input('zip', directory_from_zip,
                               'file')
        pipeline.connect_output('directory_from_zip', directory_from_zip,
                                'file')
        # Convert from NIfTI.gz to MRtrix format on output
        zip_from_directory = pipeline.create_node(
            IdentityInterface(fields=['file']), 'zip_from_directory')
        pipeline.connect_input('directory', zip_from_directory,
                               'file')
        pipeline.connect_output('zip_from_directory', zip_from_directory,
                                'file')
        pipeline.assert_connected()
        return pipeline

    _dataset_specs = set_dataset_specs(
        DatasetSpec('mrtrix', nifti_gz_format),
        DatasetSpec('nifti_gz', mrtrix_format),
        DatasetSpec('dicom', dicom_format),
        DatasetSpec('directory', directory_format),
        DatasetSpec('zip', zip_format),
        DatasetSpec('nifti_gz_from_dicom', nifti_gz_format, pipeline),
        DatasetSpec('mrtrix_from_nifti_gz', mrtrix_format, pipeline),
        DatasetSpec('nifti_from_mrtrix', nifti_format, pipeline),
        DatasetSpec('directory_from_zip', directory_format, pipeline),
        DatasetSpec('zip_from_directory', zip_format, pipeline))


class TestFormatConversions(BaseTestCase):

    def test_pipeline_prerequisites(self):
        study = self.create_study(
            ConversionStudy, 'conversion', {
                'mrtrix': Dataset('mrtrix', mrtrix_format),
                'nifti_gz': Dataset('nifti_gz', nifti_gz_format),
                'dicom': Dataset('t1_mprage_sag_p2_iso_1_ADNI', dicom_format),
                'directory': Dataset('t1_mprage_sag_p2_iso_1_ADNI',
                                     directory_format),
                'zip': Dataset('zip', zip_format)})
        study.pipeline().run(work_dir=self.work_dir)
        self.assertDatasetCreated('nifti_gz_from_dicom.nii.gz', study.name)
        self.assertDatasetCreated('mrtrix_from_nifti_gz.mif', study.name)
        self.assertDatasetCreated('nifti_from_mrtrix.nii', study.name)
        self.assertDatasetCreated('directory_from_zip', study.name)
        self.assertDatasetCreated('zip_from_directory.zip', study.name)
