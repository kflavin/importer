user_data_tmpl = """#!/bin/bash
set -vxe
sleep 1

# Ensure we halt if something goes wrong
# function cleanup {{
#   logger "Halting instance"
#   halt -p
# }}
# trap cleanup EXIT

# Configure server
sudo yum install -y awscli python3 python3-devel python36-devel python36-pip gcc mysql-devel awslogs

# Load our environment
export aws_region=$(curl http://169.254.169.254/latest/meta-data/placement/availability-zone | sed 's/[a-z]$//')

# Send cloud-init log to Cloudwatch
cat <<EOF >> /etc/awslogs/awslogs.conf
[/var/log/cloud-init-output.log]
file = /var/log/cloud-init-output.log
buffer_duration = 5000
log_stream_name = {{instance_id}}
initial_position = start_of_file
log_group_name = /var/log/cloud-init-output.log
EOF
/etc/init.d/awslogs start

# Copy packages and data from s3
aws s3 cp s3://{bucket_name}/{bucket_key} /data/
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt

# Install package
pip-3.6 install /opt/importer.tar.gz
PATH=/usr/local/bin:$PATH

# Clean and load CSV file, then mark the object as imported
ZIP_FILE=$(ls -1 /data/*.zip)
CSV_FILE=$(runner-import.py npi unzip -i $ZIP_FILE -p /data/NPPES)
CLEAN_CSV_FILE=$(runner-import.py npi preprocess -i $CSV_FILE)
time runner-import.py npi load -i $CLEAN_CSV_FILE -t {table_name} -p {period}
aws s3api put-object-tagging --bucket {bucket_name} --key {bucket_key} --tagging 'TagSet=[{{Key=imported,Value=true}}]'

# Terminate the instance
halt -p
"""