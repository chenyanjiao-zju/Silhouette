from sklearn.datasets import make_classification
from sklearn.datasets import dump_svmlight_file
import numpy as np

X, y = make_classification(**{
    'n_samples': 1000000,
    'n_features': 50,
    'n_informative': 2,
    'n_redundant': 0,
    'n_repeated': 0,
    'n_classes': 2,
    'n_clusters_per_class': 2,
    'random_state': 37
})

output = 'classification.binary'
#data = dump_svmlight_file(X, y, output)
data = dump_svmlight_file(X, y, output, zero_based=False)