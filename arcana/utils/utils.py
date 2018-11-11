from __future__ import absolute_import
from builtins import next
from builtins import range
import os
import os.path as op
import re
import shutil
from nipype.interfaces.base import (
    TraitedSpec, traits, BaseInterface, File, isdefined,
    Directory, CommandLineInputSpec, CommandLine, DynamicTraitedSpec,
    BaseInterfaceInputSpec)
from arcana.exceptions import ArcanaUsageError
from itertools import chain, groupby
from nipype.interfaces.utility.base import Merge, MergeInputSpec
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.io import IOBase, add_traits
from nipype.utils.filemanip import filename_to_list
from nipype.interfaces.base import OutputMultiPath, InputMultiPath
import numpy as np
from arcana.exceptions import ArcanaError


bash_resources = op.abspath(op.join(op.dirname(__file__), 'resources', 'bash'))

zip_path = op.join(bash_resources, 'zip.sh')
targz_path = op.join(bash_resources, 'targz.sh')
cp_path = op.join(bash_resources, 'copy_file.sh')
cp_dir_path = op.join(bash_resources, 'copy_dir.sh')
mkdir_path = op.join(bash_resources, 'make_dir.sh')


special_char_re = re.compile(r'[^\w]')


class MergeInputSpec(DynamicTraitedSpec, BaseInterfaceInputSpec):
    axis = traits.Enum(
        'vstack', 'hstack', usedefault=True,
        desc=('direction in which to merge, hstack requires same number '
              'of elements in each input'))
    no_flatten = traits.Bool(
        False, usedefault=True,
        desc='append to outlist instead of extending in vstack mode')


class MergeOutputSpec(TraitedSpec):
    out = traits.List(desc='Merged output')


class Merge(IOBase):
    """Basic interface class to merge inputs into a single list

    Examples
    --------

    >>> from nipype.interfaces.utility import Merge
    >>> mi = Merge(3)
    >>> mi.inputs.in1 = 1
    >>> mi.inputs.in2 = [2, 5]
    >>> mi.inputs.in3 = 3
    >>> out = mi.run()
    >>> out.outputs.out
    [1, 2, 5, 3]

    """
    input_spec = MergeInputSpec
    output_spec = MergeOutputSpec

    def __init__(self, numinputs=0, **inputs):
        super(Merge, self).__init__(**inputs)
        self._numinputs = numinputs
        if numinputs > 0:
            input_names = ['in%d' % (i + 1) for i in range(numinputs)]
        elif numinputs == 0:
            input_names = ['in_lists']
        else:
            input_names = []
        add_traits(self.inputs, input_names)

    def _list_outputs(self):
        outputs = self._outputs().get()
        out = []

        if self._numinputs == 0:
            values = getattr(self.inputs, 'in_lists')
            if not isdefined(values):
                return outputs
        else:
            getval = lambda idx: getattr(self.inputs, 'in%d' % (idx + 1))  # @IgnorePep8
            values = [getval(idx) for idx in range(self._numinputs)
                      if isdefined(getval(idx))]

        if self.inputs.axis == 'vstack':
            for value in values:
                if isinstance(value, list) and not self.inputs.no_flatten:
                    out.extend(value)
                else:
                    out.append(value)
        else:
            lists = [filename_to_list(val) for val in values]
            out = [[val[i] for val in lists] for i in range(len(lists[0]))]
        if out:
            outputs['out'] = out
        return outputs


class MergeTupleOutputSpec(TraitedSpec):
    out = traits.Tuple(desc='Merged output')  # @UndefinedVariable


class MergeTuple(Merge):
    """Basic interface class to merge inputs into a single tuple

    Examples
    --------

    >>> from nipype.interfaces.utility import Merge
    >>> mi = MergeTuple(3)
    >>> mi.inputs.in1 = 1
    >>> mi.inputs.in2 = [2, 5]
    >>> mi.inputs.in3 = 3
    >>> out = mi.run()
    >>> out.outputs.out
    (1, 2, 5, 3)

    """
    input_spec = MergeInputSpec
    output_spec = MergeTupleOutputSpec

    def _list_outputs(self):
        outputs = super(MergeTuple, self)._list_outputs()
        outputs['out'] = tuple(outputs['out'])
        return outputs


class Chain(IdentityInterface):

    def _list_outputs(self):
        outputs = super(Chain, self)._list_outputs()
        chained_outputs = {}
        for k, v in outputs.items():
            chained_outputs[k] = list(chain(*v))
        return chained_outputs


def dicom_fname_sort_key(fname):
    in_parts = special_char_re.split(op.basename(fname))
    out_parts = []
    for part in in_parts:
        try:
            part = int(part)
        except ValueError:
            pass
        out_parts.append(part)
    return tuple(out_parts)


class JoinPathInputSpec(TraitedSpec):
    dirname = Directory(mandatory=True, desc='directory name')
    filename = traits.Str(mandatory=True, desc='file name')


class JoinPathOutputSpec(TraitedSpec):
    path = traits.Str(mandatory=True, desc="The joined path")


class JoinPath(BaseInterface):
    """Joins a filename to a directory name"""

    input_spec = JoinPathInputSpec
    output_spec = JoinPathOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['path'] = op.join(self.inputs.dirname,
                                       self.inputs.filename)
        return outputs

    def _run_interface(self, runtime):
        return runtime


class CopyFileInputSpec(CommandLineInputSpec):
    src = File(mandatory=True, desc='source file', argstr='%s',
               position=0)
    base_dir = Directory(mandatory=True, desc='root directory', argstr='%s',
                         position=1)
    dst = File(genfile=True, argstr='%s', position=2,
               desc=("The destination file"))


class CopyFileOutputSpec(TraitedSpec):
    copied = File(exists=True, desc="The copied file")
    basedir = Directory(exists=True, desc='base directory')


class CopyFile(CommandLine):
    """Creates a copy of a given file"""

    _cmd = cp_path
    input_spec = CopyFileInputSpec
    output_spec = CopyFileOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()

        outputs['copied'] = op.join(self.inputs.base_dir, self.inputs.dst)
        outputs['basedir'] = op.join(self.inputs.base_dir)
        return outputs

    def _gen_filename(self, name):
        if name == 'copied':
            fname = op.basename(self.inputs.dst)
        else:
            assert False
        return fname


class CopyDirInputSpec(CommandLineInputSpec):
    src = File(mandatory=True, desc='source file', argstr='%s',
               position=0)
    base_dir = Directory(mandatory=True, desc='root directory', argstr='%s',
                         position=1)
    dst = File(genfile=True, argstr='%s', position=2,
               desc=("The destination file"))
    method = traits.Int(mandatory=True, desc='method', argstr='%s', position=3)


class CopyDirOutputSpec(TraitedSpec):
    copied = Directory(exists=True, desc="The copied file")
    basedir = Directory(exists=True, desc='base directory')


class CopyDir(CommandLine):
    """Creates a copy of a given file"""

    _cmd = cp_dir_path
    input_spec = CopyDirInputSpec
    output_spec = CopyDirOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        if self.inputs.method == 1:
            outputs['copied'] = op.join(self.inputs.base_dir)
            outputs['basedir'] = op.join(self.inputs.base_dir)
        elif self.inputs.method == 2:
            outputs['copied'] = op.join(self.inputs.base_dir,
                                             self._gen_filename('copied'))
            outputs['basedir'] = op.join(self.inputs.base_dir)
        return outputs

    def _gen_filename(self, name):
        if name == 'copied':
            fname = op.basename(self.inputs.dst)
        else:
            assert False
        return fname


class MakeDirInputSpec(CommandLineInputSpec):
    base_dir = Directory(mandatory=True, desc='root directory', argstr='%s',
                         position=0)
    name_dir = Directory(genfile=True, argstr='%s', position=1,
                         desc=("name of the new directory"))


class MakeDirOutputSpec(TraitedSpec):
    new_dir = Directory(exists=True, desc="The created directory")


class MakeDir(CommandLine):
    """Creates a new directory"""

    _cmd = mkdir_path
    input_spec = MakeDirInputSpec
    output_spec = MakeDirOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['new_dir'] = op.join(self.inputs.base_dir)
#                                           self._gen_filename('new_dir'))
        return outputs

    def _gen_filename(self, name):
        if name == 'new_dir':
            fname = op.basename(self.inputs.name_dir)
        else:
            assert False
        return fname


class ZipDirInputSpec(CommandLineInputSpec):
    dirname = Directory(mandatory=True, desc='directory name', argstr='%s',
                        position=1)
    zipped = File(genfile=True, argstr='%s', position=0,
                  desc=("The zipped zip file"))
    ext_prefix = traits.Str(
        mandatory=False, default='', usedefault=True,
        desc=("Extra extension to prepend before .zip is appended to "
              "file name"))


class ZipDirOutputSpec(TraitedSpec):
    zipped = File(exists=True, desc="The zipped directory")


class ZipDir(CommandLine):
    """Creates a zip repository from a given folder"""

    _cmd = zip_path
    input_spec = ZipDirInputSpec
    output_spec = ZipDirOutputSpec
    zip_ext = '.zip'

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['zipped'] = op.abspath(
            self._gen_filename('zipped'))
        return outputs

    def _gen_filename(self, name):
        if name == 'zipped':
            if isdefined(self.inputs.zipped):
                fname = self.inputs.zipped
            else:
                fname = (op.basename(self.inputs.dirname) +
                         self.inputs.ext_prefix + self.zip_ext)
        else:
            assert False
        return fname


class UnzipDirInputSpec(CommandLineInputSpec):
    zipped = Directory(mandatory=True, desc='zipped file name', argstr='%s',
                       position=0)


class UnzipDirOutputSpec(TraitedSpec):
    unzipped = Directory(exists=True, desc="The unzipped directory")


class UnzipDir(CommandLine):
    """Unzips a folder that was zipped by ZipDir"""

    _cmd = 'unzip -qo'
    input_spec = UnzipDirInputSpec
    output_spec = UnzipDirOutputSpec

    def _run_interface(self, *args, **kwargs):
        self.listdir_before = set(os.listdir(os.getcwd()))
        return super(UnzipDir, self)._run_interface(*args, **kwargs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        new_files = set(os.listdir(os.getcwd())) - self.listdir_before
        if len(new_files) > 1:
            raise ArcanaUsageError(
                "Zip repositorys can only contain a single directory, found "
                "'{}'".format("', '".join(new_files)))
        try:
            unzipped = next(iter(new_files))
        except StopIteration:
            raise ArcanaUsageError(
                "No files or directories found in unzipped directory")
        outputs['unzipped'] = op.join(os.getcwd(), unzipped)
        return outputs


class CopyToDirInputSpec(TraitedSpec):
    in_files = traits.List(
        traits.Either(File(exists=True), Directory(exists=True)),
        mandatory=True, desc='input dicom files')
    out_dir = File(desc='the output dicom file')
    extension = traits.Str(desc='specify the extention for the copied file.',
                           default='', usedefault=True)
    file_names = traits.List(
        traits.Str, desc=("The filenames to use to save the files with within "
                          "the directory"))


class CopyToDirOutputSpec(TraitedSpec):
    out_dir = Directory(exists=True, desc='the output dicom directory')


class CopyToDir(BaseInterface):
    """
    Copies a list of files of directories into a directory
    """

    input_spec = CopyToDirInputSpec
    output_spec = CopyToDirOutputSpec

    def _run_interface(self, runtime):
        dirname = self.out_dir
        os.makedirs(dirname)
        ext = self.inputs.extension
        if isdefined(self.inputs.file_names):
            if len(self.inputs.file_names) != len(self.inputs.in_files):
                raise ArcanaError(
                    "Number of provided filenames ({}) does not match number "
                    "of provided files ({})".format(
                        len(self.inputs.file_names),
                        len(self.inputs.in_files)))
        for i, f in enumerate(self.inputs.in_files):
            if isdefined(self.inputs.file_names):
                path = op.join(self.out_dir,
                               op.basename(self.inputs.file_names[i]))
                if op.isdir(f):
                    shutil.copytree(f, path)
                else:
                    shutil.copy(f, path)
            else:
                # Not quite sure what is going on here, probably should
                # ask Francesco what this logic is for
                if op.isdir(f):
                    out_name = f.split('/')[-1]
                    if ext:
                        out_name = '{0}_{1}'.format(
                            out_name, ext + str(i).zfill(3))
                    shutil.copytree(f, dirname + '/{}'.format(out_name))
                elif op.isfile(f):
                    if ext == '.dcm':
                        fname = op.join(dirname, str(i).zfill(4)) + ext
                    else:
                        fname = dirname
                    shutil.copy(f, fname)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_dir'] = self.out_dir
        return outputs

    @property
    def out_dir(self):
        if isdefined(self.inputs.out_dir):
            dpath = self.inputs.out_dir
        else:
            dpath = op.abspath('store_dir')
        return dpath


class ListDirInputSpec(TraitedSpec):
    directory = File(mandatory=True, desc='directory to read')
    filter = traits.Callable(
        desc=("A callable (e.g. function) used to filter the filenames"))
    sort_key = traits.Callable(
        desc=("A callable (e.g. function) that generates a key from the "
              "listed filenames with which to sort them with"))
    group_key = traits.Callable(
        desc=("A callable (e.g. function) that generates a key with which to"
              "group the filenames"))


class ListDirOutputSpec(TraitedSpec):
    files = traits.List(File(exists=True),
                        desc='The files present in the directory')
    groups = traits.Dict(traits.Str, traits.List(File(exists=True)),
                         desc="The files grouped by the 'group_key' function")


class ListDir(BaseInterface):
    """
    Lists all files (not sub-directories) in a directory
    """

    input_spec = ListDirInputSpec
    output_spec = ListDirOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        dname = self.inputs.directory
        outputs = self._outputs().get()
        key = self.inputs.sort_key if isdefined(self.inputs.sort_key) else None
        files = []
        for fname in os.listdir(dname):
            path = op.join(dname, fname)
            if op.isfile(path) and (not isdefined(self.inputs.filter) or
                                    self.inputs.filter(fname)):
                files.append(fname)
        files = [op.join(dname, f) for f in sorted(files, key=key)]
        if isdefined(self.inputs.group_key):
            outputs['groups'] = dict(
                (k, list(values))
                for k, values in groupby(files, key=self.inputs.group_key))
        outputs['files'] = files
        return outputs


class TarGzDirInputSpec(CommandLineInputSpec):
    dirname = Directory(mandatory=True, desc='directory name', argstr='%s',
                        position=1)
    zipped = File(genfile=True, argstr='%s', position=0,
                  desc=("The tar_gz file"))


class TarGzDirOutputSpec(TraitedSpec):
    zipped = File(exists=True, desc="The tar_gz directory")


class TarGzDir(CommandLine):
    """Creates a tar_gzip repository from a given folder"""

    _cmd = targz_path
    input_spec = TarGzDirInputSpec
    output_spec = TarGzDirOutputSpec
    targz_ext = '.tar.gz'

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['zipped'] = op.join(os.getcwd(), self._gen_filename('zipped'))
        return outputs

    def _gen_filename(self, name):
        if name == 'zipped':
            fname = op.basename(self.inputs.dirname) + self.targz_ext
        else:
            assert False
        return fname


class UnTarGzDirInputSpec(CommandLineInputSpec):

    gzipped = Directory(mandatory=True, argstr='%s', position=0,
                        desc=("The tar_gz file"))


class UnTarGzDirOutputSpec(TraitedSpec):
    gunzipped = Directory(exists=True, desc="The gunzipped directory")


class UnTarGzDir(CommandLine):
    """Unzip a folder created using TarGz"""

    _cmd = 'tar -zxvf '
    input_spec = UnTarGzDirInputSpec
    output_spec = UnTarGzDirOutputSpec

    def _run_interface(self, *args, **kwargs):
        self.listdir_before = set(os.listdir(os.getcwd()))
        return super(UnTarGzDir, self)._run_interface(*args, **kwargs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        new_files = set(os.listdir(os.getcwd())) - self.listdir_before
        if len(new_files) > 1:
            raise ArcanaUsageError(
                "Zip repositorys can only contain a single directory, found '{}'"
                .format("', '".join(new_files)))
        try:
            unzipped = next(iter(new_files))
        except StopIteration:
            raise ArcanaUsageError(
                "No files or directories found in unzipped directory")
        outputs['gunzipped'] = op.join(os.getcwd(), unzipped)
        return outputs


class SelectOneInputSpec(BaseInterfaceInputSpec):
    inlist = InputMultiPath(
        traits.Any, mandatory=True, desc='list of values to choose from')
    index = traits.Int(mandatory=True, desc='0-based indice of element to extract')


class SelectOneOutputSpec(TraitedSpec):
    out = OutputMultiPath(traits.Any, desc='selected value')


class SelectOne(IOBase):
    """Basic interface class to select specific elements from a list

    Examples
    --------

    >>> from nipype.interfaces.utility import Select
    >>> sl = Select()
    >>> _ = sl.inputs.trait_set(inlist=[1, 2, 3, 4, 5], index=[3])
    >>> out = sl.run()
    >>> out.outputs.out
    4

    >>> _ = sl.inputs.trait_set(inlist=[1, 2, 3, 4, 5], index=[3, 4])
    >>> out = sl.run()
    >>> out.outputs.out
    [4, 5]

    """

    input_spec = SelectOneInputSpec
    output_spec = SelectOneOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        out = np.array(self.inputs.inlist)[self.inputs.index]
        outputs['out'] = out
        return outputs
