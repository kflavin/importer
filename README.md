# serverless importer

## Installation and Usage

#### Pre-reqs

You should have your AWS creds configured.  To use profiles, specify `AWS_PROFILE=profile_name`

#### Install

```bash
# Configure environment
cp env.template .env.aws
vim .env.aws
# < edit values, see below >
source .env.aws

# Choose a serverless stack to deploy (one of the serverless*.yml files).  dev stacks creates an RDS instance.
cp serverless-prod.yml serverless.yml

# Build the Python script
python setup.py sdist

# Deploy stack to AWS. 
sls deploy --stage=dev

# If using a new database, you can quickly create the db and table as follows.  Otherwise skip this step.
sls invoke --function create_db --data '{ "table_name": "'$npi_table_name'", "database": "'$db_schema'" }'

# The Lambda functions are cron'd, but can be called manually using helper scripts

# Download any available zip files to S3.  Default directly is npi-in/[weekly|monthly]
./bin/download.sh dev

# Load weekly files
./bin/weekly.sh dev

# Load the most recent monthly file
./bin/monthly.sh dev
```

#### Environment values

Your environment should be configured before deploying.  If you are using the stack to create an RDS instance, then you will need to update your environment file (`db_host`) after it has been created.

```bash
# Set an AWS profile if you wish to use something other than the default.  Otherwise, don't set this.
# export AWS_PROFILE=my_profile_name

# DB settings
export db_user=''
export db_password=''
export db_host=''
export db_schema=''

export npi_table_name='npi_tmp'
export npi_log_table_name='npi_import_log_tmp'

################
## AWS Specific
################
export aws_region='us-east-1'
export aws_key=''

# Loader AMI
export aws_image_id=''  # Requires the custom Loader AMI

# Instance sizing
export aws_weekly_instance_type='t2.small'
export aws_monthly_instance_type='m5.4xlarge'

# Networking
export aws_subnets='subnet-1234,subnet-45678'
export aws_vpc_id=''
export aws_security_groups=''

# Database (only required if provisioning DB resources)
export aws_rds_security_group=''
export aws_rds_parameter_group=''

# Configuration
export weekly_import_timeout='20'   # in minutes
export monthly_import_timeout='120'
```

#### Packaging

```bash
bin
└── ...Helper Scripts...
dist
└── ...Runner distributable...
importer
└── ...Importer Library...
lambdas
└── ...Lambda Functions...
resources
└── ...AWS resources...
├── runner-import.py     # Runner for import library
├── serverless.yml       # Serverless framework
├── serverless-dev.yml   # Create an RDS instance
├── serverless-prod.yml  # Without RDS instance
└── setup.py             # Build the python importer
```

### EC2 Importer

Deploys an ephemeral EC2 instance to handle data import.

See: https://cloudcraft.co/view/a49965c0-e2ef-4819-99c1-03722a3ce26e?key=JROwMt9RjsrSxSYUovuIqw

_Fill out later_

### Dependencies

* The lambdas have a dependency on the sql resources in the importer, under `importer/sql/`
* The importer's runner has a dependency on AWS for cloudwatch logging
* AWS subnets and security groups need to be created ahead of time
* The EC2's run on a custom AMI with the environment pre-installed # todo: put this in packer

## Destroy

```bash
# S3 files must be removed, or Cloudformation cannot delete the bucket
bin/delete_bucket_files.sh dev

# Tear down stack
sls remove --stage=dev
```
