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


class MyAnalysis(MyBaseAnalysis):

    @classmethod
    def construct_menu(cls):
        menu = super().construct_menu()
        menu.add_file_input('primary', fmt.STD_IMAGE_FORMATS,
                            "The primary image to analyse")
        menu.add_file('new_deriv', fmt.nifti_gz,
                      "The multiplication of primary with 'a_param'",
                      salience=ds.PUBLICATION)
        menu.add_field('new_metric', typing.Sequence(float),
                       salience=ds.QA)
        menu.add_param('multiplier', "The multiplication factor",
                       default=2.0, salience=ps.ARBITRARY)
        menu.add_param('do_subtract', "subtract the subtractor",
                       default=False, salience=ps.CONTEXT_DEPENDENT)
        menu.add_param('subtractor', default=10.0,
                       desc="", salience=ps.ARBITRARY)
        return menu

    @pipeline_constructor('new_deriv', 'new_metric')
    def new_derivatives(self, pipeline):
        """
        Generates new_deriv and new_metric from the primary image
        """

        upstream = pipeline.add(
            self.tool('mrcalc', '3.0.1'),
            inputs={
                'in_file': self.menu['primary'],
                'operand': self.param('multiplier'),
                'op': 'multiply'},
            outputs={
                'new_deriv': 'out_file'})

        if self.param('do_subtract'):
            upstream = pipeline.add(
                self.tool('mrcalc'),
                inputs={
                    'in_file': upstream.out_file,
                    'operand': self.param('subtractor'),
                    'op': 'subtract'},
                name='second_mrcalc')

        pipeline.add(
            self.tool('mrmath'),
            inputs={
                'in_file': upstream.out_file,
                'op': 'mean'},
            outputs={
                'new_metric': 'out'})
