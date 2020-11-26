Use AWS SDK for python (boto3) to deploy cluster in AWS Elastic MapReduce.

### Dependency and preliminary

- Python3, boto3.

### Configuration

- configuration of [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)

please make sure that you have set up the AWS credentials. Namely the
~/.aws/[credential, config]

- configuration of experiment

please refer to the `experiment_conf.json`. You should configure the instance_type to be chosed from, e.g. "m4.large".

Second you should specify the pyspark program in the `algorithm` field. Then last, is the list of sample configuration.
if `partition` is not 1, then a configuration of partition of 1 would automatically launch.

> PLEASE BE CAREFUL on the experiment configuration since AWS has
its limits on vCPU acquirement at the same time. refer to 
>[this](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-on-demand-instances.html#ec2-on-demand-instances-limits)

- requirement and possible extension

we now require the pyspark args to be fixed, and this can be modified on both the `deploy_emr_cluster.py` and the
`{your_algorithm}.py`

- aws vCPU limit

AWS has a vCPU maximum limit, thus we cannot deploy many cluster simultaneously. This would cause internal error
and the cluster would fail in launch. You can manually CLONE the cluster in Amazon EMR web panel. And we provide the
data retrieving script `manual_fetch_log.py` to fetch the log from s3 to local dir.

Following the restriction, the class `SilExperiment` can be modified to schedule the deployment of clusters via threading.

- accuracy calculation

We provide the automation of Silhouette model. Please refer to `data_processing.py`.

- IAM roles and EC2 security groups

The `deploy_emr_cluster.py` has provided the _delete_ method. If IAM roles and security groups are not deleted by this
script, you can delete those in IAM and EC2 web panel. BTW, to delete EC2 security group, you'd better delete the in/out
bound first to remove the reference.