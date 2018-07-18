import os
import boto3
from pprint import pprint

user_data_tmpl = """#!/bin/bash
env >> /tmp/myenvironment
pwd >> /tmp/pwd
whoami >> /tmp/whoami
sudo yum install -y awscli python3 python3-devel python36-pip gcc mysql-devel
# aws s3 cp s3://{s3_bucket}/lib /opt/ --recursive
# aws s3 cp s3://{s3_bucket}/npidata_pfile_10k.csv data/
aws s3 cp s3://{s3_bucket}/importer.tar.gz /opt
cd /opt
tar xzvf importer.tar.gz
cd importer*
sudo pip-3.6 install -r requirements.txt
export db_user=$(aws ssm get-parameters --names "db_user" --region us-east-1 --with-decryption)
export db_password=$(aws ssm get-parameters --names "db_password" --region us-east-1 --with-decryption)
export db_host=$(aws ssm get-parameters --names "db_host" --region us-east-1 --with-decryption)
env >> /tmp/myenvironment2
./runner-import.py npi create
"""

def handler(event, context):
    print("Starting instance...")

    region = os.environ.get('aws_region', 'us-east-1')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    subnetId = os.environ.get('aws_subnet_id')
    s3_bucket = os.environ.get('s3_bucket')
    instance_profile = os.environ.get('instance_profile')

    user_data = user_data_tmpl.format(s3_bucket=s3_bucket)

    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.create_instances(
        # NetworkInterfaces=[{
        #         # 'DeviceIndex': 0,
        #         # 'AssociatePublicIpAddress': True,
        #         # 'AssociatePublicIpAddress': False,
        #         'Groups': securityGroups
        #     }],
        NetworkInterfaces=[
            {
                'DeviceIndex': 0,
                'SubnetId': subnetId,
                'AssociatePublicIpAddress': False,
                'Groups': securityGroups
            },
        ],
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
