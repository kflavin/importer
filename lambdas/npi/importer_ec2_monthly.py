import os
import boto3
from lambdas.resources.userdata import user_data_tmpl
# from lambdas.helpers.s3 import next_bucket_key, is_imported
from lambdas.helpers.db import DBHelper
from lambdas.helpers.ec2 import EC2Helper
from lambdas.periods import MONTHLY as period
from importer import weekly_prefix as bucket_prefix

def handler(event, context):
    print(f"Starting {period} import...")
    print(event)

    region = os.environ.get('aws_region')
    keyName = os.environ.get('aws_key')
    imageId = os.environ.get('aws_image_id')
    instanceType = os.environ.get('aws_instance_type')
    securityGroups = os.environ.get('aws_security_groups').split(",")
    subnetId = os.environ.get('aws_private_subnets').split(",")[0]      # Just take the first private subnet
    instance_profile = os.environ.get('aws_instance_profile')
    table_name = os.environ.get('npi_table_name', 'npi')
    log_table_name = os.environ.get('npi_log_table_name', 'npi_import_log')
    timeout = os.environ.get('monthly_import_timeout', '30')            # Default, 30 minutes
    bucket_name = os.environ.get("aws_s3_bucket")

    ec2 = EC2Helper(region, period)
    rds = DBHelper(region)

    # filename = bucket_key.split("/")[-1]
    print(f"bucket: {bucket_name} prefix: {bucket_prefix} table: {table_name} period: {period}")

    if not rds.imports_ready(log_table_name, period, 1):
        print(f"No files in {bucket_name}/{bucket_prefix} are ready for import.")
        return False

    print(f"Current number of tasks are {ec2.active_imports(table_name)}, max instances are 1")

    if ec2.active_imports(table_name) > 0:
        print(f"SKIPPING, there is already an import running for table {table_name}.")
        return False

    user_data = user_data_tmpl.format(bucket_name=bucket_name,
                                    bucket_prefix=bucket_prefix,
                                    table_name=table_name,
                                    log_table_name=log_table_name,
                                    period=period,
                                    timeout=timeout)

    # Run the instance
    instance = ec2.run(key_name, image_id, instance_type, subnet_id, user_data, instance_profile, 
                        security_groups, context.function_name, period, table_name)

    print(f"Instance: {instance}")
    return True
    
# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
