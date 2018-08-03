import os
import boto3
from backend.resources.userdata import user_data_tmpl
from backend.helpers.s3 import next_bucket_key, is_imported
from backend.helpers.ec2 import active_imports
from backend.periods import WEEKLY

def handler(event, context):
    print(f"Starting instance for {WEEKLY} import...")
    print(event)

    region = os.environ.get('aws_region')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    subnetId = os.environ.get('aws_private_subnets').split(",")[0]      # Just take the first private subnet
    instance_profile = os.environ.get('aws_instance_profile')
    table_name = os.environ.get('npi_table_name', 'npi')
    timeout = os.environ.get('weekly_import_timeout', '10')             # Default, 30 minutes
    max_concurrent_instances = int(os.environ.get('npi_max_weekly_instances', 1))

    if "Records" in event:
        print("Processing from S3 trigger")
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        bucket_key = event['Records'][0]['s3']['object']['key']
    else:
        print("Processing from cron")
        bucket_name = os.environ.get("aws_s3_bucket")
        bucket_key = next_bucket_key(bucket_name, f"npi-in/{WEEKLY}")

    filename = bucket_key.split("/")[-1]
    print(f"bucket: {bucket_name} key: {bucket_key} table: {table_name} period: {WEEKLY}")

    if is_imported(bucket_name, bucket_key):
        print(f"Skipping {bucket_name}/{bucket_key}, already imported.")
        return False

    print(f"Current number of tasks are {active_imports(table_name, WEEKLY)}, max instances are {max_concurrent_instances}")

    if active_imports(table_name, WEEKLY) >= max_concurrent_instances:
        print(f"Skipping {bucket_name}/{bucket_key}, there is a {WEEKLY} import running.")
        return False

    user_data = user_data_tmpl.format(bucket_name=bucket_name,
                                    bucket_key=bucket_key,
                                    table_name=table_name,
                                    period=WEEKLY,
                                    timeout=timeout)

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
                        'Value': WEEKLY
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
    return True


if __name__ == '__main__':
    print("Testing import from the CLI")
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
