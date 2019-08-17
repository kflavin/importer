import os
import boto3
from lambdas.helpers.ec2 import EC2Helper
from lambdas.helpers.file_loader import loader_user_data

user_data_head_tmpl = loader_user_data("setup")
user_data_body_tmpl = loader_user_data("rxnorm_body")
user_data_finish_tmpl = loader_user_data("finish")


def handler(event, context):
    print(f"Starting RXNORM import...")
    print(event)

    environment = os.environ.get('environment', 'dev')
    region = os.environ.get('aws_region')
    key_name = os.environ.get('aws_key')
    image_id = os.environ.get('aws_image_id')
    instance_type = os.environ.get('aws_instance_type')
    security_groups = os.environ.get('aws_security_groups').split(",")
    subnet_id = os.environ.get('aws_subnets').split(",")[0]  # Just use the first subnet
    instance_profile = os.environ.get('aws_instance_profile')
    table_name = os.environ.get('table_name')
    bucket_name = os.environ.get("aws_s3_bucket")
    sns_topic_arn = os.environ.get("aws_sns_topic_arn")

    ec2 = EC2Helper(region)

    active_imports = ec2.active_imports(table_name, environment)
    print(f"Current number of tasks are {active_imports}, max instances are 1")
    if active_imports > 0:
        print(f"SKIPPING, there is already an import running for table {table_name}.")
        return False

    user_data_head = user_data_head_tmpl.format(environment=environment,
                                                importer_type="RXNORM",
                                                sns_topic_arn=sns_topic_arn,
                                                bucket_name=bucket_name)

    user_data_body = user_data_body_tmpl.format(timeout=15,
                                                environment=environment,
                                                bucket_name=bucket_name)

    user_data_finish = user_data_finish_tmpl

    user_data = f"{user_data_head}\n{user_data_body}\n{user_data_finish}"

    # # Run the instance
    instance = ec2.run(key_name, image_id, instance_type, subnet_id, user_data, instance_profile,
                       security_groups, context.function_name, table_name, environment)

    print(f"Instance: {instance}")

    print("done")
    return True


# For testing from the cli
if __name__ == '__main__':
    class Object(object):
        pass


    o = Object()
    o.function_name = "importer ec2 from cli"
    handler(None, o)
