user_data_tmpl = """#!/bin/bash
set -vxe
sleep 1

# # Ensure we halt if something goes wrong
# function cleanup {{
#   logger "Halting instance due to a failure"
#   halt -p
# }}
# trap cleanup EXIT

# Load our environment.  Used by runner.
export aws_region=$(curl http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/[a-z]$//')
export instance_id=$(curl http://169.254.169.254/latest/meta-data/instance-id)
set +x
export db_user=$(aws ssm get-parameters --names "db_user" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_password=$(aws ssm get-parameters --names "db_password" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_host=$(aws ssm get-parameters --names "db_host" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_schema=$(aws ssm get-parameters --names "db_schema" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
set -x

# Install dependencies
# ami-14c5486b (3.6)
# sudo yum install -y awscli python3 python3-devel python36-devel python36-pip gcc mysql-devel awslogs || ( echo "Failed to install packages." && exit 1 )
# ami-b70554c8 (3.7)
sudo yum install -y awscli awslogs gcc python3 python3-pip python3-setuptools python3-libs python3-devel mysql-devel || ( echo "Failed to install packages." && exit 1 )

# Send logs to CloudWatch
aws s3 cp s3://{bucket_name}/config/awslogs.conf /etc/awslogs/awslogs.conf
systemctl start awslogsd
# /etc/init.d/awslogs start

# Copy data package and runner package
aws s3 cp s3://{bucket_name}/{bucket_key} /tmp/npi/
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt

# Install package
pip3 install /opt/importer.tar.gz
PATH=/usr/local/bin:$PATH

# Clean and load CSV file, then mark the object as imported
ZIP_FILE=$(ls -1 /tmp/npi/*.zip)
timeout {timeout}m runner-import.py npi all -i $ZIP_FILE -p {period} -t {table_name} -u /tmp/npi/NPPES --bucket-name {bucket_name} --bucket-key {bucket_key}
aws s3api put-object-tagging --bucket {bucket_name} --key {bucket_key} --tagging 'TagSet=[{{Key=imported,Value=true}}]'

# Terminate the instance
halt -p
"""