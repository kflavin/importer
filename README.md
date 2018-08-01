# serverless importer

## Install

```bash
# Configure environment
cp env.template .env.aws
vim .env.aws
# < edit values, see below >
source .env.aws

# Deploy stack (RDS, Lambda, S3)
sls deploy --stage=dev

# Build the Python script
python setup.py sdist

# Push script to the s3 bucket
./bin/stage_runner_to_s3.sh

# Create initial database
sls invoke --function create_db --data '{ "table_name": "'$npi_table_name'", "database": "'$db_schema'" }'

# Upload a (partial) CSV file to s3
bin/copy_data_files.sh npidata_pfile_50k.csv
```

This template creates RDS and S3 resources.

#### Environment values

```bash
export db_user=''
export db_password=''
export db_host=''
export db_schema=''
export npi_table_name=''
export aws_region=''
export aws_key=''
export aws_image_id='ami-14c5486b'
export aws_weekly_instance_type='t2.small'
export aws_monthly_instance_type='m5.4xlarge'
export aws_private_subnets='subnet1,subnet2'
export aws_public_subnets='subnet1,subnet2'
export aws_vpc_id=''
export aws_rds_security_group=''
export aws_security_groups=''
export aws_instance_profile=''  # Needs access to S3 and SSM
export aws_rds_parameter_group=''
export npi_max_weekly_instances='3'
```

### EC2 Importer

Deploys an ephemeral EC2 instance to handle data import.

See: https://cloudcraft.co/view/a49965c0-e2ef-4819-99c1-03722a3ce26e?key=JROwMt9RjsrSxSYUovuIqw

_Fill out later_

### Step Importer

_Removed_

See branch: __step_functions__

## Destroy

```bash
# S3 files must be removed, or Cloudformation cannot delete the bucket
bin/delete_bucket_files.sh

# Tear down stack
sls remove --stage=dev
```