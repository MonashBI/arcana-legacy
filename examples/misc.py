import os.path as op
from arcana import Dataset

dset2 = Dataset(
    op.expanduser('~/nipype_arcana_workshop/notebooks/data/sample-datasets/depth2'), depth=2)
print(dset2)
print('subjects:', list(dset2.subject_ids))
print('visits:', list(dset2.visit_ids))
