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

# Install dependencies
sudo yum install -y awscli python3 python3-devel python36-devel python36-pip gcc mysql-devel awslogs || ( echo "Failed to install packages." && exit 1 )

# Send logs to CloudWatch
aws s3 cp s3://{bucket_name}/config/awslogs.conf /etc/awslogs/awslogs.conf
/etc/init.d/awslogs start

# Copy data package and runner package
aws s3 cp s3://{bucket_name}/{bucket_key} /tmp/npi/
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt

# Install package
pip-3.6 install /opt/importer.tar.gz
PATH=/usr/local/bin:$PATH

# Clean and load CSV file, then mark the object as imported
ZIP_FILE=$(ls -1 /tmp/npi/*.zip)
timeout {timeout}m runner-import.py npi all -i $ZIP_FILE -p {period} -t {table_name} -u /tmp/npi/NPPES --bucket-name {bucket_name} --bucket-key {bucket_key}

# Terminate the instance
halt -p
"""