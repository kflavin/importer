user_data_tmpl = """#!/bin/bash
set -vxe
sleep 1

# Ensure we halt if something goes wrong
function cleanup {{
  logger "Halting instance due to a failure"
  sleep 10  # give some extra time to get all logs to CW
  halt -p
}}
trap cleanup EXIT

# Load our environment.  Used by runner.
export aws_region=$(curl http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/[a-z]$//')
export instance_id=$(curl http://169.254.169.254/latest/meta-data/instance-id)
set +x
export db_user=$(aws ssm get-parameters --names "db_user" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_password=$(aws ssm get-parameters --names "db_password" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_host=$(aws ssm get-parameters --names "db_host" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_schema=$(aws ssm get-parameters --names "db_schema" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
set -x

# Send logs to CloudWatch
aws s3 cp s3://{bucket_name}/config/awslogs.conf /etc/awslogs/awslogs.conf
systemctl start awslogsd

# Copy data package and runner package
# aws s3 cp s3://{bucket_name}/bucket_key /tmp/npi/
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt

# Install package
pip3 install /opt/importer.tar.gz
PATH=/usr/local/bin:$PATH

# Clean and load CSV file, then mark the object as imported
# timeout {timeout}m runner-import.py npi import -i -p {period} -t {table_name} -u /tmp/npi/NPPES
timeout {timeout}m runner-import.py npi all -t {table_name} -i {log_table_name} -p {period} -w /tmp/npi -u s3://{bucket_name}/{bucket_prefix}

# ZIP_FILE=$(ls -1 /tmp/npi/*.zip)
# timeout {timeout}m runner-import.py npi all -i $ZIP_FILE -p {period} -t {table_name} -u /tmp/npi/NPPES
# aws s3api put-object-tagging --bucket {bucket_name} --key bucket_key --tagging 'TagSet=[{{Key=imported,Value=true}}]'

# Terminate the instance
halt -p
"""