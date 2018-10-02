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
        FilesetSpec('text_from_text', text_format, 'conv_pipeline'),
        FilesetSpec('directory_from_zip_on_input', directory_format,
                    'conv_pipeline'),
        FilesetSpec('zip_from_directory_on_input', zip_format,
                    'conv_pipeline'),
        FilesetSpec('directory_from_zip_on_output', directory_format,
                    'conv_pipeline'),
        FilesetSpec('zip_from_directory_on_output', zip_format,
                    'conv_pipeline')]

    def conv_pipeline(self, **mods):
        pipeline = self.pipeline(
            name='conv_pipeline',
            mods=mods,
            desc=("A pipeline that tests out various data format "
                         "conversions"))
        # No conversion from text to text format
        text_from_text = pipeline.add('text_from_text',
                                      IdentityInterface(fields=['file']))
        pipeline.connect_input('text', text_from_text, 'file',
                               format=text_format)
        pipeline.connect_output('text_from_text', text_from_text,
                                'file')
        # Convert from zip file to directory format on input
        directory_from_zip_on_input = pipeline.add(
            'directory_from_zip_on_input', IdentityInterface(fields=['file']))
        pipeline.connect_input('zip', directory_from_zip_on_input, 'file',
                               format=directory_format)
        pipeline.connect_output('directory_from_zip_on_input',
                                directory_from_zip_on_input,
                                'file', format=directory_format)
        # Convert from zip file to directory format on input
        directory_from_zip_on_output = pipeline.add(
            'directory_from_zip_on_output', IdentityInterface(fields=['file']))
        pipeline.connect_input('zip', directory_from_zip_on_output, 'file',
                               format=zip_format)
        pipeline.connect_output('directory_from_zip_on_output',
                                directory_from_zip_on_output,
                                'file', format=zip_format)
        # Convert from directory to zip format on input
        zip_from_directory_on_input = pipeline.add(
            'zip_from_directory_on_input', IdentityInterface(fields=['file']))
        pipeline.connect_input('directory', zip_from_directory_on_input,
                               'file', format=zip_format)
        pipeline.connect_output('zip_from_directory_on_input',
                                zip_from_directory_on_input, 'file',
                                format=zip_format)
        # Convert from directory to zip format on input
        zip_from_directory_on_output = pipeline.add(
            'zip_from_directory_on_output', IdentityInterface(fields=['file']))
        pipeline.connect_input('directory', zip_from_directory_on_output,
                               'file', format=directory_format)
        pipeline.connect_output('zip_from_directory_on_output',
                                zip_from_directory_on_output, 'file',
                                format=directory_format)
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
        self.assertCreated(list(study.data('directory_from_zip_on_input'))[0])
        self.assertCreated(list(study.data('zip_from_directory_on_input'))[0])
        self.assertCreated(list(study.data('directory_from_zip_on_output'))[0])
        self.assertCreated(list(study.data('zip_from_directory_on_output'))[0])
