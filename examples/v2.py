import typing
from enum import Enum
from arcana.analysis import Analysis
from .base import MyBaseAnalysis
from . import file_formats as fmt


class DerivativeSalience(Enum):
    DEBUG = 0
    REUSE = 1
    QA = 2
    PUBLICATION = 3
    KEY = 4


class ParamSalience(Enum):
    DEBUG = 0
    DEFAULT_RECOMMENDED = 1
    CONTEXT_DEPENDENT = 2
    ARBITRARY = 3
    VALUE_REQUIRED = 4


ds = DerivativeSalience
ps = ParamSalience


class MyAnalysisClass(MyBaseAnalysisClass):

    name = 'myanalysis'

    @classmethod
    @menu_constructor
    def construct_menu(cls, menu):
        MyBaseAnalysisClass.construct_menu(menu)
        menu.set_filegroup_input('primary', fmt.STD_IMAGE_FORMATS,
                                 "The primary image to analyse")
        menu.set_filegroup('new_deriv', fmt.nifti_gz,
                           "The multiplication of primary with 'a_param'",
                           salience=ds.PUBLICATION)
        menu.set_filegroup('new_deriv2', fmt.png,
                           "The multiplication of primary with 'a_param'",
                           salience=ds.PUBLICATION)
        menu.set_filegroup('new_deriv3', fmt.mrtrix_track_format,
                           "The multiplication of primary with 'a_param'",
                           salience=ds.PUBLICATION)
        menu.set_field('new_metric', typing.Sequence(float),
                       salience=ds.QA)
        menu.set_param('multiplier', "The multiplication factor",
                       default=2.0, salience=ps.ARBITRARY)
        menu.set_switch('do_subtract', desc="subtract the subtractor",
                        default=False, salience=ps.CONTEXT_DEPENDENT)
        menu.set_param('subtractor', default=10.0,
                       desc="", salience=ps.ARBITRARY)

    @pipeline_constructor('new_deriv', 'new_metric')
    def a_pipeline(self, pipeline):
        """
        Generates new_deriv and new_metric from the primary image
        """

        upstream = pipeline.add(
            self.tool('mrcalc', '3.0.1'),
            inputs={
                'in_file': self.menu['primary'],
                'operand': self.param('multiplier'),
                'op': 'multiply'})

        if self.switch('do_subtract'):
            upstream = pipeline.add(
                self.tool('mrcalc'),
                inputs={
                    'in_file': upstream.out_file,
                    'operand': self.param('subtractor'),
                    'op': 'subtract'},
                name='second_mrcalc')

        mrmath = pipeline.add(
            self.tool('mrmath'),
            inputs={
                'in_file': upstream.out_file,
                'op': 'mean'})

        pipeline.set_outputs({
            'new_metric': (mrmath, 'out'),
            'new_deriv': (upstream, 'out_file')})

    another_pipeline = from_base(
        'new_deriv2', method=MyBaseAnalysis.a_base_pipeline)

    yet_another_pipeline = from_base(
        'new_deriv3', method=MyBaseAnalysis.another_base_pipeline,
        maps={'base_input': 'primary'})


if __name__ == '__main__':

    import subprocess as sp

    sp.call("arcana myanalysis ./path/to/dataset new_metric --input primary '.*t2.* --parameter subtractor 100.0", shell=True)
    sp.call("arcana myanalysis ./path/to/bids-dataset new_metric --parameter subtractor 100.0", shell=True)
    sp.call("arcana myanalysis MYPROJECTID new_metric --input primary '.*t2.* --parameter subtractor 100.0 --repository xnat http://mbi-xnat.erc.monash.edu.au --execution massive myuser myproject", shell=True)
    sp.call("arcana myanalysis openfmri new_metric --parameter subtractor 100.0 --repository datalad --execution aws myaccount", shell=True)
