import boto3
from pprint import pprint

def active_imports(table_name):
    client = boto3.client('ec2')

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
            'Name': 'instance-state-name',
            'Values': ['pending', 'running']
        }

    ]
    
    response = client.describe_instances(Filters=filters)
    return len(response['Reservations']) 

# Test from the CLI
if __name__ == "__main__":
    print(active_imports("npi"))
