import os
import boto3
from pprint import pprint

user_data_tmpl = """#!/bin/bash
echo "Hello World" >> /tmp/data.txt
sudo yum install -y awscli python3 python3-devel gcc mysql-devel
aws s3 cp s3://{s3_bucket}/importer importer/ --recursive
aws s3 cp s3://{s3_bucket}/npidata_pfile_10k.csv data/
cd importer
pip3 install -r requirements.txt
"""

def handler(event, context):
    print("Starting instance...")

    region = os.environ.get('aws_region', 'us-east-1')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    s3_bucket = os.environ.get('s3_bucket')
    instance_profile = os.environ.get('instance_profile')

    user_data = user_data_tmpl.format(s3_bucket=s3_bucket)

    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.create_instances(
        NetworkInterfaces=[{
                'DeviceIndex': 0,
                # 'AssociatePublicIpAddress': True,
                'AssociatePublicIpAddress': False,
                'Groups': securityGroups
            }],
        TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': context.function_name
                    },
                ]
            },
        ],
        KeyName=keyName,
        ImageId=imageId,
        InstanceType=instanceType,
        InstanceInitiatedShutdownBehavior='terminate',
        MinCount=1, MaxCount=1,
        UserData = user_data,
        IamInstanceProfile={ 'Name': instance_profile })
    
    # not yet implemented:
    #  instance will run batch script and terminate on completion

if __name__ == '__main__':
    class Object(object):
        pass
    o = Object()
    o.function_name="CLI"
    handler(None, o)
