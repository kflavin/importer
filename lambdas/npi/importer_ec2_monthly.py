import os
import boto3
from lambdas.resources.userdata import user_data_tmpl
# from lambdas.helpers.s3 import next_bucket_key, is_imported
from lambdas.helpers.db import DBHelper
from lambdas.helpers.ec2 import EC2Helper
from lambdas.periods import MONTHLY as period
from importer import monthly_prefix as bucket_prefix

def handler(event, context):
    print(f"Starting {period} import...")
    print(event)

    # Run first time to initialize database.  This will zero out deactivate NPI data.
    initialize = event.get('initialize', False)
    init_flag = "--initialize" if initialize else ""
    print(f"Initialize?: {initialize}, init_flag={init_flag}")

    environment = os.environ.get('environment', 'dev')
    region = os.environ.get('aws_region')
    key_name = os.environ.get('aws_key')
    image_id = os.environ.get('aws_image_id')
    instance_type = os.environ.get('aws_instance_type')
    security_groups = os.environ.get('aws_security_groups').split(",")
    subnet_id = os.environ.get('aws_subnets').split(",")[0]      # Just use the first subnet
    instance_profile = os.environ.get('aws_instance_profile')
    table_name = os.environ.get('npi_table_name')
    log_table_name = os.environ.get('npi_log_table_name')
    timeout = os.environ.get('monthly_import_timeout', '30')
    bucket_name = os.environ.get("aws_s3_bucket")
    sns_topic_arn = os.environ.get("aws_sns_topic_arn")

    ec2 = EC2Helper(region, period)
    rds = DBHelper(region)

    # filename = bucket_key.split("/")[-1]
    print(f"bucket: {bucket_name} prefix: {bucket_prefix} table: {table_name} period: {period}")

    if not rds.files_ready(log_table_name, period, environment, 1):
        print(f"No files in {bucket_name}/{bucket_prefix} are ready for import.")
        return False

    active_imports = ec2.active_imports(table_name, environment)
    print(f"Current number of tasks are {active_imports}, max instances are 1")
    if active_imports > 0:
        print(f"SKIPPING, there is already an import running for table {table_name}.")
        return False

    user_data = user_data_tmpl.format(bucket_name=bucket_name,
                                      bucket_prefix=bucket_prefix,
                                      environment=environment,
                                      table_name=table_name,
                                      log_table_name=log_table_name,
                                      period=period,
                                      timeout=timeout,
                                      init_flag=init_flag,
                                      limit=1,
                                      sns_topic_arn=sns_topic_arn)

    # Run the instance
    instance = ec2.run(key_name, image_id, instance_type, subnet_id, user_data, instance_profile, 
                        security_groups, context.function_name, period, table_name, environment)

    print(f"Instance: {instance}")
    return True

    
# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
