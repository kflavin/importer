import os
import boto3
from importer.resources.userdata import user_data_tmpl

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

    if not event.get('infile'):
        raise Exception("Must specify an infile parameter to load.  None given.")

    user_data = user_data_tmpl.format(s3_bucket=s3_bucket, 
                                    infile=event.get('infile'), 
                                    table_name=event.get('table_name', 'npi'))

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
