
from sklearn.datasets import make_regression
from sklearn.datasets import dump_svmlight_file
import numpy as np

X, y = make_regression(**{
    'n_samples': 1000000,
    'n_features': 50,
    'n_informative': 4,
    'n_targets': 1,
    'random_state': 37
})

output = 'regression.binary'
# data = dump_svmlight_file(X, y, output)
data = dump_svmlight_file(X, y, output, zero_based=False)
print(data)
