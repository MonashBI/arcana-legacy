from __future__ import absolute_import
from itertools import chain
from nipype.interfaces.utility.base import Merge, MergeInputSpec
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.base import (
    DynamicTraitedSpec, BaseInterfaceInputSpec, isdefined)
from nipype.interfaces.io import IOBase, add_traits
from nipype.utils.filemanip import filename_to_list
from nipype.interfaces.base import (
    TraitedSpec, traits)


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
        for k, v in outputs.iteritems():
            chained_outputs[k] = list(chain(*v))
        return chained_outputs
