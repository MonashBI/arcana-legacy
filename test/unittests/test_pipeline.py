from arcana.dataset import DatasetSpec, DatasetMatch
from arcana.data_format import (
    text_format, directory_format, zip_format)
from arcana.study.base import Study, StudyMetaClass
from arcana.testing import BaseTestCase
from nipype.interfaces.utility import IdentityInterface


class ConversionStudy(Study):

    __metaclass__ = StudyMetaClass

    add_data_specs = [
        DatasetSpec('text', text_format),
        DatasetSpec('directory', directory_format),
        DatasetSpec('zip', zip_format),
        DatasetSpec('text_from_text', text_format, 'pipeline'),
        DatasetSpec('directory_from_zip', directory_format, 'pipeline'),
        DatasetSpec('zip_from_directory', zip_format, 'pipeline')]

    def pipeline(self):
        pipeline = self.create_pipeline(
            name='pipeline',
            inputs=[DatasetSpec('text', text_format),
                    DatasetSpec('directory', directory_format),
                    DatasetSpec('zip', directory_format)],
            outputs=[DatasetSpec('text_from_text', text_format),
                     DatasetSpec('directory_from_zip', directory_format),
                     DatasetSpec('zip_from_directory', directory_format)],
            desc=("A pipeline that tests out various data format "
                         "conversions"),
            version=1,
            citations=[],)
        # No conversion from text to text format
        text_from_text = pipeline.create_node(
            IdentityInterface(fields=['file']), 'text_from_text')
        pipeline.connect_input('text', text_from_text, 'file')
        pipeline.connect_output('text_from_text', text_from_text,
                                'file')
        # Convert from zip file to directory format on input
        directory_from_zip = pipeline.create_node(
            IdentityInterface(fields=['file']), 'directory_from_zip')
        pipeline.connect_input('zip', directory_from_zip, 'file')
        pipeline.connect_output('directory_from_zip', directory_from_zip,
                                'file')
        # Convert from NIfTI.gz to MRtrix format on output
        zip_from_directory = pipeline.create_node(
            IdentityInterface(fields=['file']), 'zip_from_directory')
        pipeline.connect_input('directory', zip_from_directory, 'file')
        pipeline.connect_output('zip_from_directory',
                                zip_from_directory, 'file')
        pipeline.assert_connected()
        return pipeline


class TestFormatConversions(BaseTestCase):

    def test_pipeline_prerequisites(self):
        study = self.create_study(
            ConversionStudy, 'conversion', [
                DatasetMatch('text', text_format, 'text'),
                DatasetMatch('directory', directory_format,
                             'directory'),
                DatasetMatch('zip', zip_format, 'zip')])
        study.data('text_from_text')
        study.data('directory_from_zip')
        study.data('zip_from_directory')
        self.assertDatasetCreated('text_from_text', study.name)
        self.assertDatasetCreated('directory_from_zip', study.name)
        self.assertDatasetCreated('zip_from_directory.zip', study.name)
