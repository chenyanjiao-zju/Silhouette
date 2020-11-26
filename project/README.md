## SILHOUETTE: Efficient Cloud Configuration Exploration for Large-Scale Analytics
SILHOUETTE is a cloud configuration selection framework based on performance models for various large-scale analytics jobs with minimal training overhead.

With the development of the large-scale data analytics jobs, it is challenging to determine the best cloud configuration in some cloud services such as Amazon EC2 or Google Cloud Compute Engines, and a poor choice may incur significantly higher costs to the user.

SILHOUETTE is here to address the problem mentioned above. The essence of SILHOUETTE is to build performance prediction models with carefully selected small-scale experiments on small subsets of input data, in order to estimate the performance with the entire input data on larger cluster sizes. For more details, please see our [paper] (the link of the paper, to be changed once it is published)

**Installing SILHOUETTE**

Prerequisite: you should install Numpy, Scipy and Cvxpy whose script is as follows.

```
pip install -r project/requirements.txt
```

Now you can clone this repository to your environments.

**Using SILHOUETTE**

There are generally 3 steps to use SILHOUETTE that can be seen in the picture below.

[![DwY6II.png](https://s3.ax1x.com/2020/11/26/DwY6II.png)](https://imgchr.com/i/DwY6II)

Give a specific job, workload (size of the input data) as well as your training budget, 3 main procedures are as follows.

1. ***Training data collector*** : The training data collector first de- termines which training experiments to conduct. You could run *project/design_of_experiment.py* to implement this. Then, the training data collector runs the job on the selected experiments to obtain the execution time samples.
2. ***Model constructor*** : The model builder first builds a base prediction model fitted using the training data of one instance type obtained from the training data collector. Then the model transformer transforms the base prediction model to prediction models for other instance types. These two phases are implemented in *project/select_key_feature.py*.
3. ***Selectorbuilder*** : With prediction models, the selector builder can obtain the estimated execution time and cost for a specific job with a specific configuration. This phase is implemented in *project/configuration_selector.py*

For deploy the clusters, we recommend reading the README file in subdir `emr` and `flintrock`. And also we provide some of the demo pyspark script in `spark-mllib`. We also share some of the tools demo to better run these experiments, such as data generation, collection and execution scripts. Please refer to these subdirectories.
