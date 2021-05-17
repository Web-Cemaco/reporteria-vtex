import os
import time
import boto3
from ec2_metadata import ec2_metadata

def shutdown_4hours():
    time.sleep(14400)
    boto3_instance = boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS'),
        aws_secret_access_key=os.environ.get('AWS_SECRET'),
        region_name='us-east-1'
    )
    iid = ec2_metadata.instance_id
    ec2 = boto3_instance.client('ec2')
    ec2.terminate_instances(InstanceIds=[iid])