from sklearn.datasets import make_blobs
from sklearn.datasets import dump_svmlight_file

import numpy as np

X, y = make_blobs(**{
    'n_samples': 1000000,
    'n_features': 50,
    'centers': 4,
    'cluster_std': 1.0,
    'center_box': (-10.0, 10.0),
    'random_state': 37
})

output = 'clustering.binary'
# data = dump_svmlight_file(X, y, output)
data = dump_svmlight_file(X, y, output, zero_based=False)