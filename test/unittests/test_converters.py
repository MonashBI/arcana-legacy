import os
import tempfile
import os.path as op
from arcana.data import InputFilesetSpec, FilesetSpec, InputFilesets, Fileset
from arcana.data.file_format import (
    text_format, directory_format, zip_format)
from arcana.study.base import Study, StudyMetaClass
from arcana.utils.testing import BaseTestCase
from nipype.interfaces.utility import IdentityInterface
from arcana.utils.interfaces import ZipDir
from future.utils import with_metaclass
from unittest import TestCase


class TestConverterAvailability(TestCase):

    def test_find_converter(self):
        converter = zip_format.converter_from(directory_format)
        self.assertIsInstance(converter.interface, ZipDir)


class ConversionStudy(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [
        InputFilesetSpec('text', text_format),
        InputFilesetSpec('directory', directory_format),
        InputFilesetSpec('zip', zip_format),
        FilesetSpec('text_from_text', text_format, 'conv_pipeline'),
        FilesetSpec('directory_from_zip_on_input', directory_format,
                    'conv_pipeline'),
        FilesetSpec('zip_from_directory_on_input', zip_format,
                    'conv_pipeline'),
        FilesetSpec('directory_from_zip_on_output', directory_format,
                    'conv_pipeline'),
        FilesetSpec('zip_from_directory_on_output', zip_format,
                    'conv_pipeline')]

    def conv_pipeline(self, **name_maps):
        pipeline = self.new_pipeline(
            name='conv_pipeline',
            name_maps=name_maps,
            desc=("A pipeline that tests out various data format "
                         "conversions"))
        # No conversion from text to text format
        pipeline.add(
            'text_from_text',
            IdentityInterface(fields=['file']),
            inputs={
                'file': ('text', text_format)},
            outputs={
                'text_from_text': ('file', text_format)})
        # Convert from zip file to directory format on input
        pipeline.add(
            'directory_from_zip_on_input',
            IdentityInterface(fields=['file']),
            inputs={
                'file': ('zip', directory_format)},
            outputs={
                'directory_from_zip_on_input': ('file', directory_format)})
        # Convert from zip file to directory format on input
        pipeline.add(
            'directory_from_zip_on_output',
            IdentityInterface(fields=['file']),
            inputs={
                'file': ('zip', zip_format)},
            outputs={
                'directory_from_zip_on_output': ('file', zip_format)})
        # Convert from directory to zip format on input
        pipeline.add(
            'zip_from_directory_on_input',
            IdentityInterface(fields=['file']),
            inputs={
                'file': ('directory', zip_format)},
            outputs={
                'zip_from_directory_on_input': ('file', zip_format)})
        # Convert from directory to zip format on input
        pipeline.add(
            'zip_from_directory_on_output',
            IdentityInterface(fields=['file']),
            inputs={
                'file': ('directory', directory_format)},
            outputs={
                'zip_from_directory_on_output': ('file', directory_format)})
        return pipeline


class TestFormatConversions(BaseTestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        # Run BaseTestCase setUp
        super(TestFormatConversions, self).setUp()

    @property
    def INPUT_FILESETS(self):
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
        return Fileset.from_path(path, format=directory_format)

    @property
    def input_zip(self):
        path = op.join(self.tempdir, 'zip.zip')
        if not op.exists(path):
            # Create zip file
            zipper = ZipDir()
            zipper.inputs.dirname = self.input_directory.path
            zipper.inputs.zipped = path
            zipper.run()
        return Fileset.from_path(path, format=zip_format)

    def test_format_conversions(self):
        study = self.create_study(
            ConversionStudy, 'conversion', [
                InputFilesets('text', 'text', text_format),
                InputFilesets('directory', 'directory', directory_format),
                InputFilesets('zip', 'zip', zip_format)])
        self.assertCreated(list(study.data('text_from_text'))[0])
        self.assertCreated(list(study.data('directory_from_zip_on_input'))[0])
        self.assertCreated(list(study.data('zip_from_directory_on_input'))[0])
        self.assertCreated(list(study.data('directory_from_zip_on_output'))[0])
        self.assertCreated(list(study.data('zip_from_directory_on_output'))[0])
