user_data_tmpl = """#!/bin/bash
set -vxe

# Halt on failure
function cleanup {{
  halt -p
}}
trap cleanup EXIT

# Configure server
sudo yum install -y awscli python3 python3-devel python36-devel python36-pip gcc mysql-devel awslogs
sleep 1
aws sts get-caller-identity

# Setup some basic logging
cat <<EOF >> /etc/awslogs/awslogs.conf
[/data/importer.log]
file = /data/importer.log
buffer_duration = 5000
log_stream_name = {{instance_id}}
initial_position = start_of_file
log_group_name = /data/importer.log
EOF

# Copy packages and data
aws s3 cp s3://{bucket_name}/npi-in/NPPES_Data_Dissemination_070218_070818_Weekly.zip /data/
aws s3 cp s3://{bucket_name}/{bucket_key} /data/
aws s3 cp s3://{bucket_name}/importer.tar.gz /opt
unzip /data/NPPES_Data_Dissemination_070218_070818_Weekly.zip -d /data/NPPES
CSV_FILE=$(ls -1 /data/NPPES/npidata_pfile_* | grep -v FileHeader)

# Install package
cd /opt
tar xzvf importer.tar.gz
cd importer*
sudo pip-3.6 install -r requirements.npi.txt

# Load our environment
export db_user=$(aws ssm get-parameters --names "db_user" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_password=$(aws ssm get-parameters --names "db_password" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_host=$(aws ssm get-parameters --names "db_host" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_schema=$(aws ssm get-parameters --names "db_schema" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)

# Clean CSV file
# CLEAN_CSV_FILE=$(./runner-import.py npi preprocess -i /data/{filename})
CLEAN_CSV_FILE=$(./runner-import.py npi preprocess -i $CSV_FILE)

# Load CSV file into database
./runner-import.py npi load -i $CLEAN_CSV_FILE -t {table_name}

# Terminate the instance
halt -p
"""