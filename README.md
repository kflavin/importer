# serverless importer

This repo is used to dynamically launch EC2 instances for data loading.  There are two distinct parts:

* Lambda functions, under `lambdas/`, which launch EC2's
* An importer script, which is only used for NPI data, under `importer/`

It currently loads the following data:

* The `npis` table
* The RxNorm product data
* Database backups

Each gets its own AWS Lambda function.  See instructions at the end of this document for how to add new lambdas.

## Installation and Usage

#### Pre-reqs

To deploy the Lambda functions, you need the following:

* python3
* serverless framework
```bash
npm install serverless -g
```
* You should have your AWS creds file configured.  If you are using profiles, be sure to set your profile name in your environment file.

#### Building and Deploying the Lambda functions

Each environment corresponds to a [Cloudformation stack](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false).  Existing stacks are `rc`, `stage`, and `prod`.

```bash
# Install dependencies
npm install

# Copy the environment file
cp env.template .env.rc
cp env.template .env.stage
cp env.template .env.prod

# fill in the appropriate values
vim .env.rc
vim .env.stage
vim .env.prod

# Deploy the stack
./setup.sh rc
./setup.sh stage
./setup.sh prod
```

#### Building and Deploying the NPI import script

This repo also contains the NPI importer script.  It is built and deployed through `setup.sh`, but can also be built separately:

```bash
python setup.py sdist
./bin/stage_runner_to_s3.sh rc
```

#### Destroy a stack

This will delete all components, _including the S3 bucket_ and any staged files!:

```bash
./teardown.sh rc
./teardown.sh stage
./teardown.sh prod
```

#### Usage

The Lambda functions are cron'd, but can also be run manually in [AWS](https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions).  To run, configure an empty test event (event isn't used, all parameters are passed through the environment).

## Overview

#### Packaging

The directory structure is laid out as follows:

```bash
bin
└── ...Helper Scripts...
dist
└── ...Distributable NPI importer script...
importer
└── ...Importer Library...
lambdas
└── ...Lambda Functions...
packer
└── ...EC2 AMI...
resources
└── ...AWS resources...
├── runner-import.py              # Runner for the NPI importer
├── serverless-template.yml       # Controls Lamba deployment and AWS resources
└── setup.py                      # Build the python importer
```

#### NPI Importer

NPI data is loaded using the `runner-import.py` script.  It is automatically staged to the S3 bucket during deployment, and downloaded by the EC2 instance.

The lambdas live under `lamdbas/npi/`

See: https://cloudcraft.co/view/a49965c0-e2ef-4819-99c1-03722a3ce26e?key=JROwMt9RjsrSxSYUovuIqw

#### RxNorm Importer

The RxNORM data is loaded using the [Talend loader](https://github.com/rxvantage/data-importer-talend).  The Talend loader is a jar built in Talend, and kept in a separate repo.  It must be staged to the S3 script bucket manually, with the name `RxNorm_Loader.zip`.

The lambdas live under `lambdas/product/`

#### Database backup

The database backup runs a `mysqldump` to backup the database.

The lambdas live under `lambdas/db_backup/`

## Add a new Lambda

You can add a new lambda with a few steps.  The lambdas are responsible for launching an EC2 with any necessary environment variables.  The userdata scripts are responsible for doing your work, terminating the instance, and sending any notifications.

1. Create a new folder under `lambdas` and add a handler (Python function).   This is responsible for launching the EC2 with your custom userdata script.  You can copy from an existing folder, like `lambdas/db_backup/`
1. Add a new "body" userdata script under `lambdas/resources/userdata/`.  This is shell code which should perform your task.
   * The `start.sh` and `finish.sh` scripts should be concatenated around your "body" script to ensure the EC2 is terminated properly.  This is done inside the lambda.
1. Add the handler (function) name to `serverless.yaml`.
1. Deploy: `./setup.sh <env>`

The `start.sh` and `finish.sh` are used as wrappers around your body data script.  They ensure your EC2 is terminated, and
alerts are sent to SNS (if desired).  They also handle setting up environment variables for database access.  _Be sure to use them or your instance will not terminate properly!_

## Dependencies

* The lambdas have a dependency on the sql resources in the importer, under `importer/sql/`
* The importer script has a dependency on AWS for cloudwatch logging
* AWS subnets and security groups need to be created ahead of time

