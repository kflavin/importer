import os
import boto3
from importer.resources.userdata import user_data_tmpl

def handler(event, context):
    print("Starting instance...")

    # Handle events from buckets and those manually invoked
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        bucket_key = event['Records'][0]['s3']['object']['key']
    except:
        bucket_name = os.environ.get('s3_bucket')
        bucket_key = event.get('infile')

    # filepath = f"{bucket_name}/{bucket_key}"
    filename = bucket_key.split("/")[-1]
    print(f"Fetching {bucket_key}")

    region = os.environ.get('aws_region', 'us-east-1')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    subnetId = os.environ.get('aws_subnet_id')
    s3_bucket = os.environ.get('s3_bucket')
    instance_profile = os.environ.get('instance_profile')
    table_name = os.environ.get('npi_table_name', 'npi')

    if not filename:
        raise Exception("Must specify an filename parameter to load.  None given.")

    user_data = user_data_tmpl.format(s3_bucket=s3_bucket,
                                    # filepath=event.get('filepath'), 
                                    # filepath=filepath, 
                                    filename=filename,
                                    bucket_name=bucket_name,
                                    bucket_key=bucket_key,
                                    table_name=table_name)

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

# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
