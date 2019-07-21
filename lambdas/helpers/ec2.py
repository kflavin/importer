import boto3
from pprint import pprint

class EC2Helper(object):
    def __init__(self, region, period="none"):
        self.client = boto3.client('ec2', region_name=region)
        self.resource = boto3.resource('ec2', region_name=region)
        self.period = period

    def run(self, key_name, image_id, instance_type, subnet_id, user_data, instance_profile, security_groups, function_name, table_name, environment):
        instance_id = self.resource.create_instances(
            NetworkInterfaces=[
                {
                    'DeviceIndex': 0,
                    'SubnetId': subnet_id,
                    # 'AssociatePublicIpAddress': False,
                    'Groups': security_groups
                },
            ],
            TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': function_name
                        },
                        # {
                        #     'Key': 'file',
                        #     'Value': filename
                        # },
                        {
                            'Key': 'table_name',
                            'Value': table_name
                        },
                        {
                            'Key': 'period',
                            'Value': self.period
                        },
                        {
                            'Key': 'environment', 
                            'Value': environment
                        }
                    ]
                },
            ],
            KeyName=key_name,
            ImageId=image_id,
            InstanceType=instance_type,
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

        return instance_id

    def active_imports(self, table_name, environment):
        filters = [
            {
                'Name':'tag:table_name', 
                'Values': [table_name]
            },
            # {
            #     'Name':'tag:period', 
            #     'Values': [period]
            # },
            {
                'Name':'tag:environment', 
                'Values': [environment]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['pending', 'running']
            }

        ]
        
        response = self.client.describe_instances(Filters=filters)
        return len(response['Reservations']) 

# Test from the CLI
if __name__ == "__main__":
    helper = EC2Helper()
    print(helper.active_imports("npi"))
