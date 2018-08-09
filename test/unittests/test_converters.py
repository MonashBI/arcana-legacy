import os
import tempfile
import os.path as op
from arcana.data import FilesetSpec, FilesetMatch, Fileset
from arcana.data.file_format.standard import (
    text_format, directory_format, zip_format)
from arcana.study.base import Study, StudyMetaClass
from arcana.testing import BaseTestCase
from nipype.interfaces.utility import IdentityInterface
from arcana.interfaces.utils import ZipDir
from future.utils import with_metaclass


class ConversionStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        FilesetSpec('text', text_format),
        FilesetSpec('directory', directory_format),
        FilesetSpec('zip', zip_format),
        FilesetSpec('text_from_text', text_format, 'pipeline'),
        FilesetSpec('directory_from_zip', directory_format, 'pipeline'),
        FilesetSpec('zip_from_directory', zip_format, 'pipeline')]

    def pipeline(self):
        pipeline = self.new_pipeline(
            name='pipeline',
            inputs=[FilesetSpec('text', text_format),
                    FilesetSpec('directory', directory_format),
                    FilesetSpec('zip', directory_format)],
            outputs=[FilesetSpec('text_from_text', text_format),
                     FilesetSpec('directory_from_zip', directory_format),
                     FilesetSpec('zip_from_directory', directory_format)],
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

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        # Run BaseTestCase setUp
        super(TestFormatConversions, self).setUp()

    @property
    def INPUT_DATASETS(self):
        return {
            'text': 'text',
            'directory': self.input_directory,
            'zip': self.input_zip}

    @property
    def input_directory(self):
        path = op.join(self.tempdir, 'directory')
        if not op.exists(path):
            # Create directory
            os.makedirs(path)
            with open(op.join(path, 'dummy.txt'), 'w') as f:
                f.write('blah')
        return Fileset.from_path(path)

    @property
    def input_zip(self):
        path = op.join(self.tempdir, 'zip.zip')
        if not op.exists(path):
            # Create zip file
            zipper = ZipDir()
            zipper.inputs.dirname = self.input_directory.path
            zipper.inputs.zipped = path
            zipper.run()
        return Fileset.from_path(path)

    def test_format_conversions(self):
        study = self.create_study(
            ConversionStudy, 'conversion', [
                FilesetMatch('text', text_format, 'text'),
                FilesetMatch('directory', directory_format,
                             'directory'),
                FilesetMatch('zip', zip_format, 'zip')])
        self.assertCreated(list(study.data('text_from_text'))[0])
        self.assertCreated(list(study.data('directory_from_zip'))[0])
        self.assertCreated(list(study.data('zip_from_directory'))[0])
