### the pyspark script

We use the common pyspark program to run our experiment. And these programs are within this dir.
We collect the time consumed from stdout and filter the result via `grep`(refer to `run_steps.sh`
in dir `flintrock/`).

### synthetic data

For clustering, regression and classification, we generate synthetic data for our
experiment.

The synthetic data is generated via [scikit learn](https://scikit-learn.org/stable/index.html).
And we provide the python script for generate these data, please refer to `synthetic_*.py` under
this dir. All data files are with `libsvm` format which is identical to the `rcv1` dataset.

### run the experiment plan

We provide the `run_steps.sh` to run the experiments. You can custom some variables there in this
script file if needed. This step might cost half an hour or more. You can monitor the spark Web UI
panel if needed.

### collect data

All results would be written into sub dir `result/`, and `collect_data.sh` would average the time
cost and write final result to stdout. And also archive these results if needed.