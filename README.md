# serverless importer

This repo is used for:

* Loading the `npis` table
* Loading the RxNorm product data
* Database backups

## Installation and Usage

Each loader gets its own AWS Lambda function.  The following describes how to deploy those Lambda functions.

#### Pre-reqs

* python3
* You should have your AWS creds file configured.  If you are using profiles: `export AWS_PROFILE=profile_name`

#### Building and Installation

Each stack corresponds to an environment.  You can create as many environments as you'd like, but they must have unique names.  To create a new stack:

* Create an environment file: `cp env.template .env.<environment name>`
* Create a serverless YAML file: `cp serverless-template.yml serverless-<my new environment>.yml`

Currently deployed stacks are `rc`, `stage`, and `prod`, and can be seen in [Cloudformation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false).

The `serverless-template-dev.yml` file spins up an RDS instance for testing, while the other `serverless-template.yml` files do not.

Example:

```bash
cp env.template .env.prod
cp serverless-template.yml serverless.prod
```

To deploy:

```bash
# First run only
./setup.sh prod
source .env.prod

# Subsequent updates, use:
./bin/deploy.sh prod
```

To build and deploy the runner separately:

```bash
python setup.py sdist
./bin/stage_runner_to_s3.sh stage
```

The helper scripts in `./bin/` take the environment name as the first parameter.

#### Usage

The Lambda functions are cron'd, but you can run them manually as follows.

```bash
# Download any available zip files to S3.  Default directly is npi-in/[weekly|monthly]
./bin/downloader.sh prod

# Load weekly files
./bin/weekly.sh prod

# Load the most recent monthly file
./bin/monthly.sh prod

# Create initial DB and tables.  Dev only.
sls invoke --stage=${STAGE:-dev} --function create_db --data '{ "table_name": "'$npi_table_name'", "log_table_name": "'$npi_log_table_name'", "database": "'$db_schema'" }'
```

#### Environment values

See `env.template` for an example environment file.  The filename should be suffixed with the environment.

#### Packaging

The directory structure is laid out as follows:

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
├── runner-import.py              # Runner for import library
├── serverless-template-dev.yml   # Create an RDS instance
├── serverless-template.yml       # Without RDS instance
└── setup.py                      # Build the python importer
```

Generally speaking, changes to code in `lambdas/` requires a deploy `./bin/deploy.sh <env>`.  Changes to 
`npi/` (the runner) requires it to be rebuilt and pushed: `python setup.py sdist && ./bin/stage_runner_to_s3.sh <env>`.

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
./teardown.sh stage
```
