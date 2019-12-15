import os
from importer import monthly_prefix as bucket_prefix
from lambdas.helpers.db import DBHelper
from lambdas.helpers.ec2 import EC2Helper
from lambdas.helpers.file_loader import loader_user_data
from lambdas.periods import MONTHLY as period

user_data_head_tmpl = loader_user_data("start")
user_data_body_tmpl = loader_user_data("npi/body")
user_data_finish_tmpl = loader_user_data("finish")


def handler(event, context):
    print(f"Starting {period} import...")
    print(event)

    # Run first time to initialize database.  This will zero out deactivated NPI data.
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
    terminate_on_completion = os.environ.get("terminate_on_completion")

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

    # Configure userdata script.  This is what will run on the EC2.
    user_data_head = user_data_head_tmpl.format(environment=environment,
                                                importer_type="NPI",
                                                sns_topic_arn=sns_topic_arn,
                                                bucket_name=bucket_name,
                                                terminate_on_completion=terminate_on_completion)

    user_data_body = user_data_body_tmpl.format(bucket_name=bucket_name,
                                                bucket_prefix=bucket_prefix,
                                                environment=environment,
                                                table_name=table_name,
                                                log_table_name=log_table_name,
                                                period=period,
                                                timeout=timeout,
                                                init_flag=init_flag,
                                                limit=1)

    user_data_finish = user_data_finish_tmpl

    user_data = f"{user_data_head}\n{user_data_body}\n{user_data_finish}"

    # Run the instance
    instance = ec2.run(key_name, image_id, instance_type, subnet_id, user_data, instance_profile, 
                       security_groups, context.function_name, table_name, environment)

    print(f"Instance: {instance}")
    return True

    
# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass

    o = Object()
    o.function_name="importer ec2 from cli"
    handler(None, o)
