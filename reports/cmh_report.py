import os.path
import numpy as np
from copy import copy

data_dir = os.path.join(os.path.dirname(__file__), '_data')

targets = np.loadtxt(os.path.join(data_dir, 'target_aspree_numbers'),
                     dtype=int)


with open(os.path.join(data_dir, 'daris-to-aspree.txt')) as f:
    data = f.read()

daris2aspree = {}
aspree2daris = {}
for line in data.split('\r'):
    daris_id, _, aspree_id = line.split('\t')
    aspree_id = int(aspree_id)
    subj_id = int(daris_id.split('.')[3])
    daris2aspree[subj_id] = aspree_id
    aspree2daris[aspree_id] = subj_id

del daris2aspree[401]  # actually 426

assert len(daris2aspree) == len(aspree2daris), (
    "{} vs {}".format(len(daris2aspree), len(aspree2daris)))


def read_cmh(fname):
    with open(fname) as f:
        data = f.read()
    lines = data.split('\r')
    for i, line in enumerate(copy(lines)):
        if line.startswith('Case ID'):
            column_header = line.split('\t')[4:]
            lines.pop(i)
    rows = {}
    for line in lines:
        cols = line.split('\t')
        daris_id = cols[0]
        if daris_id.startswith('1008.2'):
            daris_id = int(daris_id.split('.')[3])
        elif daris_id.startswith('NEURO_'):
            daris_id = int(daris_id[6:])
        elif daris_id.startswith('NEU'):
            daris_id = int(daris_id[3:])
        else:
            raise Exception(
                "Unrecognised id '{}' on line '{}'".format(daris_id, line))
        aspree_id = cols[1]
        if aspree_id == '':
            aspree_id = None
            print "{} doesn't have an aspree id".format(daris_id)
        else:
            try:
                aspree_id = int(aspree_id)
                if daris2aspree[daris_id] != aspree_id:
                    print(
                        "Mismatching ids for {}: {} and {} using daris".format(
                            daris_id, aspree_id, daris2aspree[daris_id]))
                    aspree_id = daris2aspree[daris_id]
                    continue
            except ValueError:
                raise Exception(
                    "Unrecognised aspree id {}".format())
        assert len(column_header) == len(cols[4:]), (
            "{} v {}".format(len(column_header), len(cols[4:])))
        vals = [(int(v) if v != '' else 0) for v in cols[4:]]
        rows[aspree_id] = dict(zip(column_header, vals))
    return rows, column_header

definite, def_header = read_cmh(os.path.join(data_dir, 'definite.txt'))
possible, pos_header = read_cmh(os.path.join(data_dir, 'possible.txt'))

definite_outstr = ''
possible_outstr = ''
for tgt in targets:
    try:
        definite_outstr += '\t'.join(str(definite[tgt][h])
                                     for h in def_header) + '\n'
    except KeyError:
        definite_outstr += '\t'.join([''] * len(def_header)) + '\n'
    try:
        possible_outstr += '\t'.join(str(definite[tgt][h])
                                         for h in def_header) + '\n'
    except KeyError:
        possible_outstr += '\t'.join([''] * len(def_header)) + '\n'

with open(os.path.join(data_dir, 'definite-out'), 'w') as f:
    f.write(definite_outstr)

with open(os.path.join(data_dir, 'possible-out'), 'w') as f:
    f.write(possible_outstr)

print '\t'.join(def_header)
print 'done'
