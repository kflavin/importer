import boto3
from pprint import pprint

def active_imports(table_name, period):
    client = boto3.client('ec2')

    filters = [
        {
            'Name':'tag:table_name', 
            'Values': [table_name]
        },
        {
            'Name':'tag:period', 
            'Values': [period]
        },
        {
            'Name': 'instance-state-name',
            'Values': ['pending', 'running']
        }

    ]
    
    response = client.describe_instances(Filters=filters)

    # if len(response['Reservations']) > 0:
    #     # An import EC2 of this type is running
    #     return True
    # else:
    #     return False
    return len(response['Reservations']) 

if __name__ == "__main__":
    print(active_imports("npi", "weekly"))
