import os
import time
import boto3
from ec2_metadata import ec2_metadata

#time.sleep(14400)

cemaco_session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS'),
    aws_secret_access_key=os.environ.get('AWS_SECRET'),
    region_name='us-east-1'
)

iid = ec2_metadata.instance_id
ec2 = cemaco_session.client('ec2')
ec2.terminate_instances(InstanceIds=[iid])