#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy, math, os.path, shutil, errno


# # Set up dummy project

# In[2]:


# Create a test project with dummy data
NUM_SUBJECTS = 3
NUM_VISITS = 2

HEIGHT_MEAN = 1700
HEIGHT_STD = 150
WEIGHT_MEAN = 70
WEIGHT_STD = 25
HEAD_CIRC_MEAN = 570
HEAD_CIRC_STD = 30

subjects = ['subject{}'.format(i) for i in range(NUM_SUBJECTS)]
visits = ['visit{}'.format(i) for i in range(NUM_VISITS)]

project_dir = os.path.join(os.environ['HOME'], 'Desktop', 'arcana_tutorial')
# Clean old directory
shutil.rmtree(project_dir, ignore_errors=True)
os.mkdir(project_dir)
for subj in subjects:
    for visit in visits:
        session_dir = os.path.join(project_dir, subj, visit)
        try:
            os.makedirs(session_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        with open(os.path.join(session_dir, 'metrics.txt'), 'w') as f:
            f.write('height {}\n'.format(numpy.random.randn() * HEIGHT_STD + HEIGHT_MEAN))
            f.write('weight {}\n'.format(numpy.random.randn() * WEIGHT_STD + WEIGHT_MEAN))
            f.write('head_circ {}\n'.format(numpy.random.randn() * HEAD_CIRC_STD + HEAD_CIRC_MEAN))
print("Created project in {} directory".format(project_dir))


# # Create interface for 'grep' tool

# In[3]:


from nipype.interfaces.base import (
    TraitedSpec, traits, File, isdefined,
    CommandLineInputSpec, CommandLine)

class GrepInputSpec(CommandLineInputSpec):
    match_str = traits.Str(argstr='-e %s', position=0,
                           desc="The string to search for")
    in_file = File(argstr='%s', position=1,
                   desc="The file to search")
    out_file = File(genfile=True, argstr='> %s', position=2,
                    desc=("The file to contain the search results"))


class GrepOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc="The search results")


class Grep(CommandLine):
    """Creates a zip archive from a given folder"""

    _cmd = 'grep'
    input_spec = GrepInputSpec
    output_spec = GrepOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._gen_filename('out_file')
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            if isdefined(self.inputs.out_file):
                fname = self.inputs.out_file
            else:
                fname = 'search_results.txt'
        else:
            assert False
        return os.path.abspath(fname)


# In[4]:


grep = Grep()
grep.inputs.in_file = os.path.join(project_dir, 'subject0', 'visit0', 'metrics.txt')
grep.inputs.match_str = 'height'
results = grep.run()
print(results.outputs)
print(os.getcwd())
print(os.listdir(os.getcwd()))


# In[5]:


for subj in subjects:
    for visit in visits:
        grep = Grep()
        grep.inputs.match_str = 'height'
        grep.inputs.in_file = os.path.join(project_dir, subj, visit, 'metrics.txt')
        grep.inputs.out_file = os.path.join(project_dir, subj, visit, 'grep.txt')
        result = grep.run()
        print('Processed {}'.format(result.outputs.out_file))


# # Create interface for 'awk' tool

# In[6]:


from nipype.interfaces.base import (
    TraitedSpec, traits, File, isdefined,
    CommandLineInputSpec, CommandLine)

class AwkInputSpec(CommandLineInputSpec):
    format_str = traits.Str(argstr="'%s'", position=0,
                            desc="The string to search for")
    in_file = File(argstr='%s', position=1,
                   desc="The file to parse")
    out_file = File(genfile=True, argstr='> %s', position=2,
                    desc=("The file to contain the parsed results"))


class AwkOutputSpec(TraitedSpec):
    out_file = File(exists=True, desc="The parsed results")


class Awk(CommandLine):
    """Creates a zip archive from a given folder"""

    _cmd = 'awk'
    input_spec = AwkInputSpec
    output_spec = AwkOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['out_file'] = self._gen_filename('out_file')
        return outputs

    def _gen_filename(self, name):
        if name == 'out_file':
            if isdefined(self.inputs.out_file):
                fname = self.inputs.out_file
            else:
                fname = 'awk_results.txt'
        else:
            assert False
        return os.path.abspath(fname)


# In[7]:


for subj in subjects:
    for visit in visits:
        awk = Awk()
        awk.inputs.format_str = '{print $2}'
        awk.inputs.in_file = os.path.join(project_dir, subj, visit, 'grep.txt')
        awk.inputs.out_file = os.path.join(project_dir, subj, visit, 'awk.txt')
        result = awk.run()
        print('Processed {}'.format(result.outputs.out_file))


# # Example Matlab interface

# In[9]:


from nipype.interfaces.base import traits
from nipype.interfaces.base import TraitedSpec
from nipype.interfaces.matlab import MatlabCommand, MatlabInputSpec


class HelloWorldInputSpec(MatlabInputSpec):
    name = traits.Str(mandatory=True,
                      desc='Name of person to say hello to')


class HelloWorldOutputSpec(TraitedSpec):
    matlab_output = traits.Str()


class HelloWorld(MatlabCommand):
    """Basic Hello World that displays Hello <name> in MATLAB

    Returns
    -------

    matlab_output : capture of matlab output which may be
                    parsed by user to get computation results

    Examples
    --------

    >>> hello = HelloWorld()
    >>> hello.inputs.name = 'hello_world'
    >>> out = hello.run()
    >>> print out.outputs.matlab_output
    """
    input_spec = HelloWorldInputSpec
    output_spec = HelloWorldOutputSpec

    def _my_script(self):
        """This is where you implement your script"""
        script = """
        disp('Hello %s Python')
        two = 1 + 1
        """ % (self.inputs.name)
        return script

    def run(self, **inputs):
        # Inject your script
        self.inputs.script = self._my_script()
        results = super(MatlabCommand, self).run(**inputs)
        stdout = results.runtime.stdout
        # Attach stdout to outputs to access matlab results
        results.outputs.matlab_output = stdout
        return results

    def _list_outputs(self):
        outputs = self._outputs().get()
        return outputs


# In[10]:


# hello = HelloWorld()
# hello.inputs.name = 'hello_world'
# out = hello.run()
# print(out.outputs.matlab_output)


# # Utility 'concat' Python interface

# In[12]:


from nipype.interfaces.base import (
    TraitedSpec, traits, BaseInterface, File, isdefined, InputMultiPath)

class ConcatFloatsInputSpec(TraitedSpec):
    in_files = InputMultiPath(desc='file name', mandatory=True)


class ConcatFloatsOutputSpec(TraitedSpec):
    out_list = traits.List(traits.Float, desc='input floats')


class ConcatFloats(BaseInterface):
    """Joins values from a list of files into a single list"""

    input_spec = ConcatFloatsInputSpec
    output_spec = ConcatFloatsOutputSpec

    def _list_outputs(self):
        out_list = []
        for path in self.inputs.in_files:
            with open(path) as f:
                val = float(f.read())
                out_list.append(val)
        outputs = self._outputs().get()
        outputs['out_list'] = out_list
        return outputs

    def _run_interface(self, runtime):
        # Do nothing
        return runtime


# # Python interface using Numpy

# In[13]:


from nipype.interfaces.base import (
    TraitedSpec, traits, BaseInterface)

class ExtractMetricsInputSpec(TraitedSpec):
    in_list = traits.List(traits.Float, desc='input floats')


class ExtractMetricsOutputSpec(TraitedSpec):
    std = traits.Float(desc="The standard deviation")
    avg = traits.Float(desc="The average")


class ExtractMetrics(BaseInterface):
    """Joins values from a list of files into a single list"""

    input_spec = ExtractMetricsInputSpec
    output_spec = ExtractMetricsOutputSpec

    def _list_outputs(self):
        values = self.inputs.in_list
        outputs = self._outputs().get()
        outputs['std'] = numpy.std(values)
        outputs['avg'] = numpy.average(values)
        return outputs

    def _run_interface(self, runtime):
        # Do nothing
        return runtime


# # Manual run concatenation and metric extraction over project

# In[14]:


in_files = []
for subj in subjects:
    for visit in visits:
        in_files.append(os.path.join(project_dir, subj, visit, 'awk.txt'))

concat_floats = ConcatFloats()
concat_floats.inputs.in_files = in_files
result = concat_floats.run()
print('Output list {}'.format(result.outputs.out_list))

extract_metrics = ExtractMetrics()
extract_metrics.inputs.in_list = result.outputs.out_list
result = extract_metrics.run()
print('Average: {}'.format(result.outputs.avg))
print('Std.: {}'.format(result.outputs.std))


# # NiPype workflow for 

# In[15]:


from nipype.pipeline import engine as pe  # pypeline engine
from nipype.interfaces.utility import IdentityInterface, Merge
from nipype.interfaces.io import DataGrabber

# Create workflow
workflow = pe.Workflow(name='my_workflow')

# Create subjects iterator
subject_iterator = pe.Node(IdentityInterface(['subject_id']),
                           name='subject_iterator')
workflow.add_nodes([subject_iterator])
subject_iterator.iterables = ('subject_id', subjects)

# Create visits iterator
visit_iterator = pe.Node(IdentityInterface(['visit_id']),
                         name='visit_iterator')
workflow.add_nodes([visit_iterator])
visit_iterator.iterables = ('visit_id', visits)

# Create data grabber
datasource = pe.Node(
    interface=DataGrabber(
        infields=['subject_id', 'visit_id'], outfields=['metrics']),
    name='datasource')
datasource.inputs.template = "%s/%s/metrics.txt"
datasource.inputs.base_directory = project_dir
datasource.inputs.sort_filelist = True
datasource.inputs.template_args = {'metrics': [['subject_id', 'visit_id']]}
workflow.add_nodes([datasource])

# Create grep node
grep = pe.Node(Grep(), name='grep')
grep.inputs.match_str = 'height'
workflow.add_nodes([grep])

# Create awk node
awk = pe.Node(Awk(), name='awk')
awk.inputs.format_str = '{print $2}'
workflow.add_nodes([awk])

# Merge subject and visit iterators
merge_visits = pe.JoinNode(Merge(1), name='merge_visits', joinfield='in1',
                           joinsource='visit_iterator')
merge_subjects = pe.JoinNode(Merge(1), name='merge_subjects', joinfield='in1',
                    joinsource='subject_iterator')
merge_subjects.inputs.ravel_inputs = True
workflow.add_nodes([merge_subjects, merge_visits])
                                            
# Concat floats node
concat = pe.Node(ConcatFloats(), name='concat')
workflow.add_nodes([concat])
                                            
# Extract metrics Node
extract_metrics = pe.Node(ExtractMetrics(), name='extract')
workflow.add_nodes([extract_metrics])
                                            
# Connect Nodes together                          
workflow.connect(subject_iterator, 'subject_id', datasource, 'subject_id')
workflow.connect(visit_iterator, 'visit_id', datasource, 'visit_id')
workflow.connect(datasource, 'metrics', grep, 'in_file')
workflow.connect(grep, 'out_file', awk, 'in_file')
workflow.connect(awk, 'out_file', merge_visits, 'in1')
workflow.connect(merge_visits, 'out', merge_subjects, 'in1')
workflow.connect(merge_visits, 'out', concat, 'in_files')
workflow.connect(concat, 'out_list', extract_metrics, 'in_list')
             

# Run workflow
workflow.run()


# In[ ]:


workflow.write_graph()


# # Example Arcana Study

# In[ ]:


import arcana
from arcana import (
    Study, StudyMetaClass, DatasetSpec, DatasetMatch, FieldSpec, FieldMatch,
    LocalArchive, LinearRunner, MultiStudy, MultiStudyMetaClass, ParameterSpec)
from nianalysis.data_format import text_format


# Create the Study class, defining its constituent data and option specifications

# In[ ]:


class MyStudy(Study, metacalss=StudyMetaClass): 
    
    add_data_specs = [
        DatasetSpec('a', text_format),
        DatasetSpec('b', text_format)
    ]
    
    add_parameter_specs = [
        ParameterSpec('my_param1', 1.0),
        ParameterSpec('my_param2', 'coca')
    ]
    
    def pipeline_one(self, **kwargs):
        pipeline = self.new_pipeline()
        return pipeline


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


#

