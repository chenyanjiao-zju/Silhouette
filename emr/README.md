Use AWS SDK for python (boto3) to deploy cluster in AWS Elastic MapReduce.

### Configuration

- configuration of [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)

> please make sure that you have set up the AWS credentials. Namely the
~/.aws/[credential, config]

- configuration of experiment

> please refer to the `experiment_conf.json`. You should configure the instance_type to be chosed from, e.g. "m4.large".

> Second you should specify the pyspark program in the `algorithm` field. Then last, is the list of sample configuration.
> if `partition` is not 1, then a configuration of partition of 1 would automatically launch.

>> PLEASE BE CAREFUL on the experiment configuration since AWS has
>its limits on vCPU acquirement at the same time. refer to 
>[this](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-on-demand-instances.html#ec2-on-demand-instances-limits)

- requirement and possible extend

> we now require the pyspark args to be fixed, and this can be modified on both the `deploy_emr_cluster.py` and the
> `{your_algorithm}.py`