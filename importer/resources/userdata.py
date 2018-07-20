user_data_tmpl = """#!/bin/bash

# Configure server
sudo yum install -y awscli python3 python3-devel python36-devel python36-pip gcc mysql-devel
# aws s3 cp s3://{s3_bucket}/lib /opt/ --recursive
aws s3 cp s3://{s3_bucket}/{infile} /data/
aws s3 cp s3://{s3_bucket}/importer.tar.gz /opt
cd /opt
tar xzvf importer.tar.gz
cd importer*
sudo pip-3.6 install -r requirements.txt

# Load our environment
export db_user=$(aws ssm get-parameters --names "db_user" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_password=$(aws ssm get-parameters --names "db_password" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_host=$(aws ssm get-parameters --names "db_host" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export db_schema=$(aws ssm get-parameters --names "db_schema" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)

# Remove columns
CLEAN_CSV_FILE=$(./runner-import.py npi preprocess -i /data/{infile})

# Load data
./runner-import.py npi load -i /data/$CLEAN_CSV_FILE -t {table_name}

# halt -p
"""