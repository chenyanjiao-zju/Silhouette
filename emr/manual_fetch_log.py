# Author: Xu Wang (wangxu298@whu.edu.cn)

"""
Purpose
AWS limits the maximum vCPUs running simultaneously, thus causes the cluster to terminated with errors(Internal error).
This script makes it easy to fetch the log from s3 to local dir if you manually launched a cluster, with only specifying
the cluster name and cluster id.
"""

import boto3
import logging
import gzip
import sys

logger = logging.getLogger(__name__)


def fetch_log(cluster_tuple):
    """ fetch log from s3 log, pretty similar code in deploy_emr_cluster.EMRCluster.fetchResult """
    s3 = boto3.resource("s3")
    client = boto3.client("s3")
    mybucket = s3.Bucket('silhouette-rcv1')
    cluster_name = cluster_tuple[0]
    cluster_id = cluster_tuple[1]
    logger.info(f'Try to fetch {cluster_name} log info from s3')
    target_path_prefix = f'logs/{cluster_name}/{cluster_id}/containers'
    all_obj = client.list_objects(Bucket='silhouette-rcv1', Prefix=target_path_prefix)
    application_prefix = all_obj.get('Contents', [])[0].get('Key').split('/')[4]
    obj_prefix = application_prefix.split('_')[1]
    container_prefix = 'container_' + obj_prefix + '_0001_01_000001'
    target_obj = target_path_prefix + '/' + application_prefix + '/' + container_prefix + '/stdout.gz'
    target_file_name = f'logs/{cluster_name}-stdout.gz'
    try:
        logger.info(f'Download {cluster_id} log to local directory')
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
        write_file = open(f'logs/{cluster_name}', 'w+')
        write_file.write(result)
        # for content in all_obj.get('Contents', []):
        #     file = content.get('Key')
        #     print(file)
        # s3.meta.client.download_file('silhouette-rcv1', f'{bucket_name}/{file}', f'{file}')
        # mybucket.download_file(f'{file}', f'./logs/download.gz')
    except:
        logger.error(f'{cluster_name} has no stdout output')
        print(f'{target_obj} does not exists. Maybe the cluster has encountered errors.')


def fetch_all(cluster_file):
    """ read the file and fetch, each has {CLUSTER_NAME} {CLUSTER_ID} separated by space """
    cluster_list = []
    f = open(cluster_file)
    while True:
        line = f.readline()
        if line:
            cluster_list.append(tuple(line.split()))
        else:
            break
    f.close()
    for cluster in cluster_list:
        fetch_log(cluster)


def main():
    fetch_all("manual_cluster_list.txt")


if __name__ == '__main__':
    main()
