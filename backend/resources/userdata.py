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
aws sts get-caller-identity
export db_user=$(aws ssm get-parameters --names "db_user" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_password=$(aws ssm get-parameters --names "db_password" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_host=$(aws ssm get-parameters --names "db_host" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_schema=$(aws ssm get-parameters --names "db_schema" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)


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
cd /opt
tar xzvf importer.tar.gz
cd importer*
sudo pip-3.6 install -r requirements.npi.txt

# Clean and load CSV file
ZIP_FILE=$(ls -1 /data/*.zip)
CSV_FILE=$(./runner-import.py npi unzip -i $ZIP_FILE -p /data/NPPES)
CLEAN_CSV_FILE=$(./runner-import.py npi preprocess -i $CSV_FILE)
time ./runner-import.py npi load -i $CLEAN_CSV_FILE -t {table_name} -p {period}

# Mark the object as imported
aws s3api put-object-tagging --bucket {bucket_name} --key {bucket_key} --tagging 'TagSet=[{{Key=imported,Value=true}}]'

# Terminate the instance
halt -p
"""