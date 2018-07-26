# serverless importer

[](https://cloudcraft.co/view/a49965c0-e2ef-4819-99c1-03722a3ce26e?key=JROwMt9RjsrSxSYUovuIqw&interactive=true&embed=true)

## Install

```bash
# Configure environment
cp env.template .env.aws
vim .env.aws
# < edit values, see below >
source .env.aws

# Deploy stack (RDS, Lambda, S3)
sls deploy --stage=dev

# Create initial database
sls invoke --function create_db

# Upload a (partial) data file to s3
bin/copy_data_files.sh npidata_pfile_50k.csv
```

This template creates RDS and S3 resources.

The step function isn't automatically created through the resources yet.  You will need to create the step function
manually with __resources/npi_state_machine.json__ and set the ARN to point to the __npi_step_importer__ function.

#### Environment values

```bash
export db_user='your db user'
export db_password='your db password'
export db_host='RDS endpoint'
export db_schema='database name'
export table_name='tablename'
export aws_region='us-east-1'
export aws_key='Your AWS SSH key'
export aws_security_groups='Security group for EC2 and Lambda'
export aws_private_subnets='sub1,sub2'
export aws_public_subets='sub1,sub2'
export aws_rds_security_group='Security group for RDS'
# The following are for the EC2 function only, not needed for Step function
export aws_instance_profile='IAM role with S3 access must be preconfigured'
export aws_image_id='ami-14c5486b'
export aws_instance_type='t2.small'
```

### Step Importer

Lambda + Step functions to import

Function name: npi_step_importer

Call step function inside the AWS console with the following:

```json
{
  "filename": "npidata_pfile_50k.csv",
  "chunk_size": 2500
}
```

___TODO: Split the full file into smaller files.  Lambda has a 512MB filesystem limit and cannot download the full file at once.___

### EC2 Importer

Deploys an ephemeral EC2 instance to handle data import.

** Not yet functional **


## Destroy

```bash
# S3 files must be removed, or Cloudformation cannot delete the bucket
bin/delete_bucket_files.sh

# Tear down stack
sls remove --stage=dev
```