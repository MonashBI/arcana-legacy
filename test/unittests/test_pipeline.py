from nianalysis.dataset import DatasetSpec, DatasetMatch
from mbianalysis.data_format import (
    nifti_gz_format, mrtrix_format, dicom_format, directory_format, zip_format,
    nifti_format)
from nianalysis.study.base import Study, StudyMetaClass
from nianalysis.testing import BaseTestCase
from nipype.interfaces.utility import IdentityInterface


class ConversionStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('mrtrix', nifti_gz_format),
        DatasetSpec('nifti_gz', mrtrix_format),
        DatasetSpec('dicom', dicom_format),
        DatasetSpec('directory', directory_format),
        DatasetSpec('zip', zip_format),
        DatasetSpec('nifti_gz_from_dicom', nifti_gz_format, 'pipeline'),
        DatasetSpec('mrtrix_from_nifti_gz', mrtrix_format, 'pipeline'),
        DatasetSpec('nifti_from_mrtrix', nifti_format, 'pipeline'),
        DatasetSpec('directory_from_zip', directory_format, 'pipeline'),
        DatasetSpec('zip_from_directory', zip_format, 'pipeline')]

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
            desc=("A pipeline that tests out various data format "
                         "conversions"),
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


class TestFormatConversions(BaseTestCase):

    def test_pipeline_prerequisites(self):
        study = self.create_study(
            ConversionStudy, 'conversion', [
                DatasetMatch('mrtrix', mrtrix_format, 'mrtrix'),
                DatasetMatch('nifti_gz', nifti_gz_format, 'nifti_gz'),
                DatasetMatch('dicom', dicom_format, 't1_mprage_sag_p2_iso_1_ADNI'),
                DatasetMatch('directory', directory_format, 't1_mprage_sag_p2_iso_1_ADNI'),
                DatasetMatch('zip', zip_format, 'zip')])
        study.data('nifti_gz_from_dicom')
        study.data('mrtrix_from_nifti_gz')
        study.data('nifti_from_mrtrix')
        study.data('directory_from_zip')
        study.data('zip_from_directory')
        self.assertDatasetCreated('nifti_gz_from_dicom.nii.gz', study.name)
        self.assertDatasetCreated('mrtrix_from_nifti_gz.mif', study.name)
        self.assertDatasetCreated('nifti_from_mrtrix.nii', study.name)
        self.assertDatasetCreated('directory_from_zip', study.name)
        self.assertDatasetCreated('zip_from_directory.zip', study.name)
