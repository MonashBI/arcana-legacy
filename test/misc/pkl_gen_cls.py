from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import os
import shutil
from arcana import (
    StudyMetaClass, Study, DirectoryRepository, LinearProcessor, FilesetSpec,
    FilesetMatch)
from arcana.data.file_format.standard import text_format
import pickle as pkl
import os.path as op
from nipype.interfaces.utility import IdentityInterface
from future.utils import with_metaclass

DATA_DIR = op.join(op.dirname(__file__), 'data', 'gen_cls')
ARCHIVE_DIR = op.join(DATA_DIR, 'repository')
WORK_DIR = op.join(DATA_DIR, 'work')
GEN_PKL_PATH = op.join(DATA_DIR, 'gen.pkl')
NORM_PKL_PATH = op.join(DATA_DIR, 'normal.pkl')

shutil.rmtree(DATA_DIR, ignore_errors=True)
os.makedirs(WORK_DIR)

SESS_DIR = op.join(ARCHIVE_DIR, 'SUBJECT', 'SESSION')
os.makedirs(SESS_DIR)
with open(op.join(SESS_DIR, 'fileset.txt'), 'w') as f:
    f.write('blah blah')

#     name : str
#         The name of the pipeline
#     study : Study
#         The study from which the pipeline was created
#     inputs : List[FilesetSpec|FieldSpec]
#         The list of input filesets required for the pipeline
#         un/processed filesets, and the parameters used to generate them for
#         unprocessed filesets
#     outputs : List[FilesetSpec|FieldSpec]
#         The list of outputs (hard-coded names for un/processed filesets)
#     citations : List[Citation]
#         List of citations that describe the workflow and should be cited in
#         publications
#     version : int
#         A version number for the pipeline to be incremented whenever the output
#         of the pipeline
#     prefix : str
#         Prefix prepended to the name of the pipeline. Typically passed
#         in from a kwarg of the pipeline constructor method to allow
#         multi-classes to alter the name of the pipeline to avoid name
#         clashes
#     add_inputs : List[FilesetSpec|FieldSpec]
#         Additional inputs to append to the inputs argument. Typically
#         passed in from a kwarg of the pipeline constructor method to
#         allow sub-classes to add additional inputs
#     add_outputs : List[FilesetSpec|FieldSpec]
#         Additional outputs to append to the outputs argument. Typically
#         passed in from a kwarg of the pipeline constructor method to
#         allow sub-classes to add additional outputs


class NormalClass(with_metaclass(StudyMetaClass, Study)):

    add_data_specs = [FilesetSpec('fileset', text_format),
                      FilesetSpec('out_fileset', text_format,
                                  'a_pipeline')]

    def a_pipeline(self):
        pipeline = self.pipeline(
            'a_pipeline',
            inputs=[FilesetSpec('fileset', text_format)],
            outputs=[FilesetSpec('out_fileset', text_format)],
            desc='a dummy pipeline',
            references=[],
            version=1)
        ident = pipeline.add('ident', IdentityInterface(['fileset']))
        pipeline.connect_input('fileset', ident, 'fileset')
        pipeline.connect_output('out_fileset', ident, 'fileset')
        return pipeline


GeneratedClass = StudyMetaClass(
    'GeneratedClass', (NormalClass,), {})


norm = NormalClass('norm', DirectoryRepository(ARCHIVE_DIR),
                   LinearProcessor(WORK_DIR),
                   inputs=[FilesetMatch('fileset', text_format,
                                           'fileset')])


gen = GeneratedClass('gen', DirectoryRepository(ARCHIVE_DIR),
                     LinearProcessor(WORK_DIR),
                     inputs=[FilesetMatch('fileset', text_format,
                                          'fileset')])

print(norm)
print(gen)


with open(NORM_PKL_PATH, 'w') as f:
    pkl.dump(norm, f)

with open(GEN_PKL_PATH, 'w') as f:
    pkl.dump(gen, f)

del gen, GeneratedClass

with open(NORM_PKL_PATH) as f:
    renorm = pkl.load(f)

with open(GEN_PKL_PATH) as f:
    regen = pkl.load(f)

regen.data('out_fileset')

print(regen)
