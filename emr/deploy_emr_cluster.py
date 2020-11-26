# AUTHOR: Xu Wang (wangxu298@whu.edu.cn)

"""
Purpose
Automation of deploying an AWS EMR cluster and executing predefined spark
application.
"""

import logging
import json
import time
import sys
import threading
import boto3
import gzip
from botocore.exceptions import ClientError
import emr_basics

logger = logging.getLogger(__name__)


def status_poller(intro, done_status, func):
    """
    Polls a function for status, sleeping for 10 seconds between each query,
    until the specified status is returned.
    :param intro: An introductory sentence that informs the reader what we're
                  waiting for.
    :param done_status: The status we're waiting for. This function polls the status
                        function until it returns the specified status.
    :param func: The function to poll for status. This function must eventually
                 return the expected done_status or polling will continue indefinitely.
    """
    emr_basics.logger.setLevel(logging.WARNING)
    status = None
    print(intro)
    print("Current status: ", end='')
    while status != done_status:
        prev_status = status
        status = func()
        if prev_status == status:
            print('.', end='')
        else:
            print(status, end='')
        sys.stdout.flush()
        time.sleep(10)
    print()
    emr_basics.logger.setLevel(logging.INFO)


def create_roles(job_flow_role_name, service_role_name, iam_resource):
    """
    Creates IAM roles for the job flow and for the service.
    The job flow role is assumed by the cluster's Amazon EC2 instances and grants
    them broad permission to use services like Amazon DynamoDB and Amazon S3.
    The service role is assumed by Amazon EMR and grants it permission to use various
    Amazon EC2, Amazon S3, and other actions.
    For demo purposes, these roles are fairly permissive. In practice, it's more
    secure to restrict permissions to the minimum needed to perform the required
    tasks.
    :param job_flow_role_name: The name of the job flow role.
    :param service_role_name: The name of the service role.
    :param iam_resource: The Boto3 IAM resource object.
    :return: The newly created roles.
    """
    try:
        job_flow_role = iam_resource.create_role(
            RoleName=job_flow_role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2008-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ec2.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }]
            })
        )
        waiter = iam_resource.meta.client.get_waiter('role_exists')
        waiter.wait(RoleName=job_flow_role_name)
        logger.info("Created job flow role %s.", job_flow_role_name)
    except ClientError:
        logger.exception("Couldn't create job flow role %s.", job_flow_role_name)
        raise

    try:
        job_flow_role.attach_policy(
            PolicyArn=
            "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
        )
        logger.info("Attached policy to role %s.", job_flow_role_name)
    except ClientError:
        logger.exception("Couldn't attach policy to role %s.", job_flow_role_name)
        raise

    try:
        job_flow_inst_profile = iam_resource.create_instance_profile(
            InstanceProfileName=job_flow_role_name)
        job_flow_inst_profile.add_role(RoleName=job_flow_role_name)
        logger.info(
            "Created instance profile %s and added job flow role.", job_flow_role_name)
    except ClientError:
        logger.exception("Couldn't create instance profile %s.", job_flow_role_name)
        raise

    try:
        service_role = iam_resource.create_role(
            RoleName=service_role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2008-10-17",
                "Statement": [{
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "elasticmapreduce.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }]
            })
        )
        waiter = iam_resource.meta.client.get_waiter('role_exists')
        waiter.wait(RoleName=service_role_name)
        logger.info("Created service role %s.", service_role_name)
    except ClientError:
        logger.exception("Couldn't create service role %s.", service_role_name)
        raise

    try:
        service_role.attach_policy(
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
        )
        logger.info("Attached policy to service role %s.", service_role_name)
    except ClientError:
        logger.exception(
            "Couldn't attach policy to service role %s.", service_role_name)
        raise

    return job_flow_role, service_role


def delete_roles(roles):
    """
    Deletes the roles created for this demo.
    :param roles: The roles to delete.
    """
    try:
        for role in roles:
            for policy in role.attached_policies.all():
                role.detach_policy(PolicyArn=policy.arn)
            for inst_profile in role.instance_profiles.all():
                inst_profile.remove_role(RoleName=role.name)
                inst_profile.delete()
            role.delete()
            logger.info("Detached policies and deleted role %s.", role.name)
    except ClientError:
        logger.exception("Couldn't delete roles %s.", [role.name for role in roles])
        raise


def create_security_groups(prefix, ec2_resource):
    """
    Creates Amazon EC2 security groups for the instances contained in the cluster.
    When the cluster is created, Amazon EMR adds all required rules to these
    security groups. Because this demo needs only the default rules, it creates
    empty security groups and lets Amazon EMR fill them in.
    :param prefix: The name prefix for the security groups.
    :param ec2_resource: The Boto3 Amazon EC2 resource object.
    :return: The newly created security groups.
    """
    try:
        default_vpc = list(ec2_resource.vpcs.filter(
            Filters=[{'Name': 'isDefault', 'Values': ['true']}]))[0]
        logger.info("Got default VPC %s.", default_vpc.id)
    except ClientError:
        logger.exception("Couldn't get VPCs.")
        raise
    except IndexError:
        logger.exception("No default VPC in the list.")
        raise

    groups = {'manager': None, 'worker': None}
    for group in groups.keys():
        try:
            groups[group] = default_vpc.create_security_group(
                GroupName=f'{prefix}-{group}', Description=f"EMR {group} group.")
            logger.info(
                "Created security group %s in VPC %s.",
                groups[group].id, default_vpc.id)
        except ClientError:
            logger.exception("Couldn't create security group.")
            raise

    return groups


def delete_security_groups(security_groups):
    """
    Deletes the security groups used by the demo. When there are dependencies
    on a security group, it cannot be deleted. Because it can take some time
    to release all dependencies after a cluster is terminated, this function retries
    the delete until it succeeds.
    :param security_groups: The security groups to delete.
    """
    try:
        for sg in security_groups.values():
            sg.revoke_ingress(IpPermissions=sg.ip_permissions)
        max_tries = 5
        while True:
            try:
                for sg in security_groups.values():
                    sg.delete()
                break
            except ClientError as error:
                max_tries -= 1
                if max_tries > 0 and \
                        error.response['Error']['Code'] == 'DependencyViolation':
                    logger.warning(
                        "Attempt to delete security group got DependencyViolation. "
                        "Waiting for 10 seconds to let things propagate.")
                    time.sleep(10)
                else:
                    raise
        logger.info("Deleted security groups %s.", security_groups)
    except ClientError:
        logger.exception("Couldn't delete security groups %s.", security_groups)
        raise


class SilExperiment:
    """ The global experiment design and config class"""
    _data = {}

    def __init__(self, config_file):
        self.config_file_ = config_file

    def readConfig(self):
        """
        Read config from experiment design file and carry experiments.
        """
        with open(self.config_file_, 'r') as file:
            self._data = json.load(file)

    def getInstanceTypes(self):
        return self._data['instance_type']

    def getExpSetting(self):
        return self._data['design']

    def getAlgorithm(self):
        return self._data['algorithm']

    def getDataset(self):
        return self._data['dataset']

    def emrThreadWrapper(self, instance, algo, plan):
        """ here we use a wrapper to threading the jobs concurrently """
        machine_num = plan['cores'] / instance['cores']  # here we omit the problem of remainder
        machine_num += 1  # the master node will not involve the calculation
        single_step = EMRStep(algo['name'], plan['partition'], algo['program'], algo['dataset'])
        single_cluster = EMRCluster(instance, machine_num, plan['partition'], single_step)
        single_cluster.run()
        logger.info("Running a cluster of %s", single_cluster.getClusterName())

    def dispatchJobs(self):
        """ Dispatch the jobs to run under different setting
        Here we get |INSTANCE_TYPE| * |ALGORITHM| * |PLAN| permutations of configurations
        """
        threads = []
        for instance in self._data['instance_type']:
            for algo in self._data['algorithm']:
                for plan in self._data['design']:
                    t = threading.Thread(target=self.emrThreadWrapper, args=(instance, algo, plan))
                    threads.append(t)
                    t.start()
                    # if plan['partition'] != 1:
                    #     plan['partition'] = 1
                    #     t_1 = threading.Thread(target=self.emrThreadWrapper, args=(instance, algo, plan))
                    #     threads.append(t_1)
                    #     t_1.start()
        print(f'waiting for threads to complete.... Total {len(threads)} threads running')
        for t in threads:
            t.join()
        print("All jobs completed!!!")


class EMRStep:
    """ The AWS EMR step """
    _step_name = ""
    _script_url = ""
    # _script_args: algorithm part dataset
    _algo_name = ""
    _part = 1
    _dataset = ""

    def __init__(self, algo_name, part, url, dataset):  # if there are more args to pass TODO: More args
        self._algo_name = algo_name
        self._part = part
        self._script_url = url
        self._dataset = dataset
        dataset_suffix = dataset.split('/')[-1]
        self._step_name = self._algo_name + '-' + str(self._part) + '-' + dataset_suffix

    def getStep(self):
        return {
            'name': self._step_name,
            'script_uri': self._script_url,
            'script_args': [
                str(self._part),
                str(256),  # TODO: fixed num parts 256
                self._dataset
            ]  # TODO: Now args changed to : partition num_parts input_file output_file
        }

    def getStepName(self):
        return self._step_name

    def getAlgorithm(self):
        return self._algo


class EMRCluster:
    """ The AWS EMR cluster """
    _cluster_name = ""
    _ec2_type = {}
    _num = 0
    _part = 0
    _started = False
    _cluster_id = ""
    _step = {}  # support only one step for one cluster
    _max_retry = 5

    def __init__(self, ec2, num, part, step):
        self._ec2_type = ec2['name']
        self._num = int(num)
        self._part = float(part)
        self._step = step.getStep()
        ec2_name = ec2['name']
        ec2_cores = int(ec2['cores']) * num
        # naming: TYPE-{CORE NUM}-{INSTANCE NUM}-PART-ALGORITHM-DATASET
        print(f'{ec2_name}' + '-' + f'{str(ec2_cores)}')
        print(num)
        print(step.getStepName())
        self._cluster_name = ec2_name + '-' + str(ec2_cores) + '-' + str(num) + '-' + step.getStepName()

    def getClusterName(self):
        """ return the cluster name """
        return self._cluster_name

    def run_job_flow_wrapper(self, bucket_name, keep_alive, applications, steps,
                             job_flow_role,
                             service_role,
                             security_groups,
                             emr_client):
        """ the emr.emr_basics.run_job_flow wrapper: for the config of instance type and number"""
        if self._started:
            raise
        else:

            try:
                response = emr_client.run_job_flow(
                    Name=self._cluster_name,
                    LogUri=f's3://{bucket_name}/logs/{self._cluster_name}',
                    ReleaseLabel='emr-5.30.1',
                    Instances={
                        'MasterInstanceType': self._ec2_type,
                        'SlaveInstanceType': self._ec2_type,
                        'InstanceCount': self._num,
                        'KeepJobFlowAliveWhenNoSteps': keep_alive,
                        'EmrManagedMasterSecurityGroup': security_groups['manager'].id,
                        'EmrManagedSlaveSecurityGroup': security_groups['worker'].id,
                    },
                    Steps=[{
                        'Name': step['name'],
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit', '--deploy-mode', 'cluster',
                                     step['script_uri'], *step['script_args']]
                        }
                    } for step in steps],
                    Applications=[{
                        'Name': app
                    } for app in applications],
                    JobFlowRole=job_flow_role.name,
                    ServiceRole=service_role.name,
                    EbsRootVolumeSize=10,
                    #  VisibleToAllUsers=True
                )
                cluster_id = response['JobFlowId']
                logger.info("Created cluster %s.", self._cluster_id)
            except ClientError:
                logger.exception("Couldn't create cluster.")
                raise
            else:
                return cluster_id

    def fetchResult(self):
        """ fetch the running time of spark step from logs, write result to folder ./logs/CLUSTER_NAME """
        s3 = boto3.resource("s3")
        client = boto3.client("s3")
        mybucket = s3.Bucket('silhouette-rcv1')
        logger.info(f'Try to fetch {self._cluster_name} log info from s3')
        target_path_prefix = f'logs/{self._cluster_name}/{self._cluster_id}/containers'
        all_obj = client.list_objects(Bucket='silhouette-rcv1', Prefix=target_path_prefix)
        application_prefix = all_obj.get('Contents', [])[0].get('Key').split('/')[4]
        obj_prefix = application_prefix.split('_')[1]
        container_prefix = 'container_' + obj_prefix + '_0001_01_000001'
        target_obj = target_path_prefix + '/' + application_prefix + '/' + container_prefix + '/stdout.gz'
        target_file_name = f'logs/{self._cluster_name}-stdout.gz'
        try:
            logger.info(f'Download {self._cluster_id} log to local directory')
            mybucket.download_file(target_obj,
                                   target_file_name)
            f = gzip.open(target_file_name, 'rb')
            # file_content = f.read()
            # print(file_content)
            first_line = f.readline()
            print(first_line)
            encoding = 'utf-8'
            temp = first_line.decode(encoding)
            result = temp.split(':')[1].strip()
            print(result)
            f.close()

            # write to file for further analysis
            write_file = open(f'logs/{self._cluster_name}', 'w+')
            write_file.write(result)
            # for content in all_obj.get('Contents', []):
            #     file = content.get('Key')
            #     print(file)
            # s3.meta.client.download_file('silhouette-rcv1', f'{bucket_name}/{file}', f'{file}')
            # mybucket.download_file(f'{file}', f'./logs/download.gz')
        except:
            logger.error(f'{self._cluster_name} has no stdout output')
            print(f'{target_obj} does not exists. Maybe the cluster has encountered errors.')

    def run(self):
        # lets start the new job flow, the name is generated by self, as well as log_uri
        s3_resource = boto3.resource('s3')
        iam_resource = boto3.resource('iam')
        emr_client = boto3.client('emr')
        ec2_resource = boto3.resource('ec2')
        # set up resources for cluster
        # we use the first created bucket in user's s3 service
        bucket_name = "silhouette-rcv1"  # here we use the fixed s3 bucket rather that creating a new bucket
        bucket = s3_resource.Bucket(bucket_name)
        job_flow_role, service_role = create_roles(
            f'{self._cluster_name[0:50]}-ec2-role', f'{self._cluster_name[0:50]}-service-role', iam_resource
        )
        secutiry_groups = create_security_groups(self._cluster_name, ec2_resource)
        script_key = ""
        output_prefix = f'{self._cluster_name}'
        #  this_step = {  # TODO fill the algo config
        #      'name': 'kmeans',  # TODO: here is the algo name
        #      'script_uri': f's3://{bucket_name}/{script_key}',
        #      'script_args':
        #          [
        #              '--partitions', '3', '--output_uri', f's3://{bucket_name}/{output_prefix}'
        #          ]
        #  }
        time.sleep(10)  # sleep for 10 seconds if we have iam control
        while True:
            try:
                self._cluster_id = self.run_job_flow_wrapper(
                    f'{bucket_name}',
                    False, ['Hadoop', 'Spark'], [self._step],
                    job_flow_role, service_role, secutiry_groups, emr_client)
                print(f"Running job flow for cluster {self._cluster_id}")
                break
            except ClientError as error:
                self._max_retry -= 1
                if self._max_retry > 0 and \
                        error.response['Error']['Code'] == 'ValidationException':
                    print("Instance profile is not ready, wait for another 10 seconds")
                    time.sleep(10)
                else:
                    raise

        status_poller(
            "Waiting for cluster, this typically takes several minutes...",
            'RUNNING',
            lambda: emr_basics.describe_cluster(self._cluster_id, emr_client)['Status']['State'],
        )
        status_poller(
            "Waiting for step to complete...",
            'PENDING',
            lambda: emr_basics.list_steps(self._cluster_id, emr_client)[0]['Status']['State'])
        status_poller(
            "Waiting for cluster to terminate.",
            'TERMINATED',
            lambda: emr_basics.describe_cluster(self._cluster_id, emr_client)['Status']['State']
        )
        print(f"Job complete!. The script, logs, and output for this demo are in "
              f"Amazon S3 bucket {bucket_name}. The output is:")
        for obj in bucket.objects.filter(Prefix=output_prefix):
            print(obj.get()['Body'].read().decode())

        # delete roles and security groups
        # delete_security_groups(secutiry_groups)
        # delete_roles([job_flow_role, service_role])
        # logger.info("Deleting security groupd and roles")

        # Get the time of running, we choose to retrieve the stdout of ec2 instance from logs
        # naming: s3://{bucket_name}/logs/{self._cluster_name}/{cluster_id}/containers/application_160410
        #  application_1604063314445_0001/container_1604063314445_0001_01_000001/stdout.gz
        self.fetchResult()


def checkCredential():
    """ We check if the aws account is valid using the s3 bucket, since we use the fixed bucket here """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('silhouette-rcv1')
    creation_date = my_bucket.creation_date
    if creation_date is not None:
        return True
    else:
        return False


def main():
    """ The main function """
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    if not checkCredential():
        raise
    else:
        silhouette = SilExperiment("experiment_conf.json")
        silhouette.readConfig()
        silhouette.dispatchJobs()


if __name__ == '__main__':
    main()
