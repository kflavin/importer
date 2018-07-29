import os
import boto3
from backend.resources.userdata import user_data_tmpl
from backend.helpers.s3 import next_bucket_key, is_imported
from backend.helpers.ec2 import active_import
from backend.periods import MONTHLY

def handler(event, context):
    print(f"Starting instance for {MONTHLY} import...")
    print(event)

    region = os.environ.get('aws_region', 'us-east-1')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    subnetId = os.environ.get('aws_private_subnets').split(",")[0]      # Just take the first private subnet
    instance_profile = os.environ.get('aws_instance_profile')
    table_name = os.environ.get('npi_table_name', 'npi')

    if "Records" in event:
        print("Processing from S3 trigger")
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        bucket_key = event['Records'][0]['s3']['object']['key']
    else:
        # Handle events from a Cron
        print("Processing from cron")
        bucket_name = os.environ.get("aws_s3_bucket")
        bucket_key = next_bucket_key(bucket_name, f"npi-in/{MONTHLY}")

    filename = bucket_key.split("/")[-1]
    print(f"bucket: {bucket_name} key: {bucket_key} table: {table_name} period: {MONTHLY}")

    if is_imported(bucket_name, bucket_key):
        print(f"Skipping {bucket_name}/{bucket_key}, file has already been imported.")
        return

    if active_imports(table_name, MONTHLY) > 0:
        print(f"Skipping {bucket_name}/{bucket_key}, there is a {MONTHLY} import EC2 running.")
        return

    user_data = user_data_tmpl.format(bucket_name=bucket_name,
                                    bucket_key=bucket_key,
                                    table_name=table_name,
                                    period=MONTHLY)

    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.create_instances(
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
                    {
                        'Key': 'file',
                        'Value': filename
                    },
                    {
                        'Key': 'table_name',
                        'Value': table_name
                    },
                    {
                        'Key': 'period',
                        'Value': MONTHLY
                    }
                ]
            },
        ],
        KeyName=keyName,
        ImageId=imageId,
        InstanceType=instanceType,
        InstanceInitiatedShutdownBehavior='terminate',
        MinCount=1, MaxCount=1,
        UserData = user_data,
        BlockDeviceMappings=[{
            "DeviceName": "/dev/xvda",
            "Ebs" : { 
                "VolumeSize" : 20,
                "DeleteOnTermination": True 
                }
            }],
        IamInstanceProfile={ 'Name': instance_profile })

    print(f"Instance: {instance}")
    
# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
