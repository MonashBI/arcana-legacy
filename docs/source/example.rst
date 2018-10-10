Examples
========

A basic toy example

.. code-block:: python

    from arcana.data import FilesetSelector, FilesetSpec
    from arcana.data.file_format.standard import text_format
    from arcana.study.base import Study, StudyMetaClass
    from nipype.interfaces.base import (  # @IgnorePep8
        BaseInterface, File, TraitedSpec, traits, isdefined)
    from arcana.parameter import ParameterSpec
    from nipype.interfaces.utility import IdentityInterface

    class ExampleStudy(Study):
    
        __metaclass__ = StudyMetaClass
    
        add_data_specs = [
            FilesetSpec('one', text_format),
            FilesetSpec('ten', text_format),
            FilesetSpec('derived1_1', text_format, 'pipeline1'),
            FilesetSpec('derived1_2', text_format, 'pipeline1'),
            FilesetSpec('derived2', text_format, 'pipeline2'),
            FilesetSpec('subject_summary', text_format,
                        'subject_summary_pipeline',
                        frequency='per_subject')]
    
        add_parameter_specs = [
            ParameterSpec('pipeline_option', False)]
    
        def pipeline1(self, **kwargs):
            pipeline = self.new_pipeline(
                name='pipeline1',
                inputs=[FilesetSpec('one', text_format)],
                outputs=[FilesetSpec('derived1_1', text_format),
                         FilesetSpec('derived1_2', text_format)],
                desc="A dummy pipeline used to test 'run_pipeline' method",
                version=1,
                citations=[],
                **kwargs)
            if not self.parameter('pipeline_option'):
                raise Exception("Pipeline option was not cascaded down to "
                                "pipeline1")
            ident = pipeline.create_node(IdentityInterface(['file']),
                                          name="ident1")
            ident2 = pipeline.create_node(IdentityInterface(['file']),
                                           name="ident2")
            # Connect inputs
            pipeline.connect_input('one', ident, 'file')
            pipeline.connect_input('one', ident2, 'file')
            # Connect outputs
            pipeline.connect_output('derived1_1', ident, 'file')
            pipeline.connect_output('derived1_2', ident2, 'file')
            return pipeline
    
        def pipeline2(self, **kwargs):
            pipeline = self.new_pipeline(
                name='pipeline2',
                inputs=[FilesetSpec('one', text_format),
                        FilesetSpec('derived1_1', text_format)],
                outputs=[FilesetSpec('derived2', text_format)],
                desc="A dummy pipeline used to test 'run_pipeline' method",
                version=1,
                citations=[],
                **kwargs)
            if not self.parameter('pipeline_option'):
                raise Exception("Pipeline option was not cascaded down to "
                                "pipeline2")
            math = pipeline.create_node(TestMath(), name="math")
            math.inputs.op = 'add'
            # Connect inputs
            pipeline.connect_input('one', math, 'x')
            pipeline.connect_input('derived1_1', math, 'y')
            # Connect outputs
            pipeline.connect_output('derived2', math, 'z')
            return pipeline
    
        def subject_summary_pipeline(self, **kwargs):
            pipeline = self.new_pipeline(
                name="subject_summary",
                inputs=[FilesetSpec('one', text_format)],
                outputs=[FilesetSpec('subject_summary', text_format)],
                desc=("Test of project summary variables"),
                version=1,
                citations=[],
                **kwargs)
            math = pipeline.create_join_visits_node(
                TestMath(), joinfield='x', name='math')
            math.inputs.op = 'add'
            # Connect inputs
            pipeline.connect_input('one', math, 'x')
            # Connect outputs
            pipeline.connect_output('subject_summary', math, 'z')
            pipeline.assert_connected()
            return pipeline
            
which can then be instantiated and used to generate 'derived2' with 

.. code-block:: python


    study = ExampleStudy(
        name='dummy',
        archive=LocalArchive('/path/to/local/archive'),
        processor=LinearProcessor('/my/work/dir'),
        inputs=[
            FilesetSelector('one', text_format, 'one'),
            FilesetSelector('ten', text_format, 'ten')],
        parameters={'pipeline_option': True})
    derived_filesets = study.data('derived2')
    for fileset in derived_filesets:
        print("Generated derived file '{}'.format(fileset.path))
            
where *TestMath* is defined (in typical NiPype fashion as)

.. code-block:: python


    class TestMathInputSpec(TraitedSpec):
    
        x = traits.Either(traits.Float(), traits.File(exists=True),
                          traits.List(traits.Float),
                          traits.List(traits.File(exists=True)),
                          mandatory=True, desc='first arg')
        y = traits.Either(traits.Float(), traits.File(exists=True),
                          mandatory=False, desc='second arg')
        op = traits.Str(mandatory=True, desc='operation')
    
        z = traits.File(genfile=True, mandatory=False,
                        desc="Name for output file")
    
        as_file = traits.Bool(False, desc="Whether to write as a file",
                              usedefault=True)
    
    
    class TestMathOutputSpec(TraitedSpec):
    
        z = traits.Either(traits.Float(), traits.File(exists=True),
                          'output')
    
    
    class TestMath(BaseInterface):
        """
        A basic interface to test out the pipeline infrastructure
        """
    
        input_spec = TestMathInputSpec
        output_spec = TestMathOutputSpec
    
        def _run_interface(self, runtime):
            return runtime
    
        def _list_outputs(self):
            
            x = self._load_file(x)
            y = self._load_file(y)
            oper = getattr(operator, self.inputs.op)
            if isdefined(y):
                z = oper(x, y)
            elif isinstance(x, list):
                if isinstance(x[0], basestring):
                    x = [self._load_file(u) for u in x]
                z = reduce(oper, x)
            else:
                raise Exception(
                    "If 'y' is not provided then x needs to be list")
            outputs = self.output_spec().get()
            z_path = op.abspath(self._gen_z_fname())
            with open(z_path, 'w') as f:
                f.write(str(z))
            outputs['z'] = z_path
            return outputs
    
        def _gen_filename(self, name):
            if name == 'z':
                fname = self._gen_z_fname()
            else:
                assert False
            return fname
    
        def _gen_z_fname(self):
            if isdefined(self.inputs.z):
                fname = self.inputs.z
            else:
                fname = 'z.txt'
            return fname
    
        @classmethod
        def _load_file(self, path):
            with open(path) as f:
                return float(f.read())
