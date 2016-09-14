from nipype.pipeline import engine as pe
from nipype.interfaces.utility import Function


def print_inputs(**kwargs):
    print '\nNode inputs\n--------'
    print '\n'.join('{}: {}'.format(k, v) for k, v in kwargs.items())
    print '---------\n'
    return_vals = [kwargs[k] for k in sorted(kwargs)]
    if len(return_vals) == 1:
        return_vals = return_vals[0]
    return return_vals


class Dummy(Function):

    def __init__(self, *args):
        sorted_args = sorted(args)
        super(Dummy, self).__init__(
            input_names=sorted_args, output_names=sorted_args,
            function=print_inputs)


c = pe.JoinNode(Dummy('all_sessions_per_subject', 'subjects'),
                name='c', joinsource='b',
                joinfield=['all_sessions_per_subject'])
d = pe.JoinNode(Dummy('all_subjects', 'all_sessions'), name='d',
                joinsource='a',
                joinfield=['all_subjects', 'all_sessions'])
a = pe.Node(Dummy('subjects'), name='a')
a.iterables = ('subjects', [1, 2, 3, 4, 5])
b = pe.Node(Dummy('subjects', 'sessions'), name='b')
b.iterables = ('sessions', [.1, .2, .3])

workflow = pe.Workflow(name='test')
workflow.add_nodes([a, b, c])
workflow.connect(a, 'subjects', b, 'subjects')
workflow.connect(b, 'subjects', c, 'subjects')
workflow.connect(b, 'sessions', c, 'all_sessions_per_subject')
workflow.connect(c, 'subjects', d, 'all_subjects')
workflow.connect(c, 'all_sessions_per_subject', d, 'all_sessions')

workflow.run()
