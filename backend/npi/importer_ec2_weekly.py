import os
import boto3
from backend.resources.userdata import user_data_tmpl

def handler(event, context):
    print("Starting instance...")

    # Handle events from buckets and those invoked manually
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        bucket_key = event['Records'][0]['s3']['object']['key']
    except:
        bucket_name = os.environ.get('aws_s3_bucket')
        bucket_key = event.get('infile')

    period = "weekly"
    filename = bucket_key.split("/")[-1]
    region = os.environ.get('aws_region', 'us-east-1')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    subnetId = os.environ.get('aws_private_subnets').split(",")[0]      # Just take the first private subnet
    instance_profile = os.environ.get('aws_instance_profile')
    table_name = os.environ.get('npi_table_name', 'npi')

    if not filename:
        raise Exception("Must specify an filename parameter to load.  None given.")

    print(f"bucket: {bucket_name} key: {bucket_key} table: {table_name} period: {period}")
    user_data = user_data_tmpl.format(s3_bucket=bucket_name,
                                    filename=filename,
                                    bucket_name=bucket_name,
                                    bucket_key=bucket_key,
                                    table_name=table_name,
                                    period=period)

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
                        'Key': 'type',
                        'Value': "npi-weekly"
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
        IamInstanceProfile={ 'Name': instance_profile })

    print(f"Instance: {instance}")
    

# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
