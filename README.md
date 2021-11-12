# serverless importer

This repo is used to dynamically launch EC2 instances for data loading. There are two distinct parts:

- Lambda functions, under `lambdas/`, which launch EC2's
- An importer script, which is only used for NPI data, under `importer/`

It currently loads the following data:

- The `npis` table
- The `products` and `product_synonyms` tables
- Database backups (WIP)

Each gets its own AWS Lambda function (or set of functions). See instructions at the end of this document for how to add new lambdas.

## Installation and Usage

#### Pre-reqs

To deploy the Lambda functions, you need the following:

- python 3.6.0
- serverless framework
- AWS access (keys should be configured)
- Docker (required by non-Linux users to build for AWS Lambda, which runs on Linux)

```bash
npm install serverless -g
```

- You should have your AWS creds file configured. If you are using profiles, be sure to set your profile name in your environment file.

#### Initial Setup

Each environment corresponds to a [Cloudformation stack](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false). Existing stacks are `rc`, `staging`, and `prod`.

```bash
# Install dependencies
npm install

# Copy the environment file, and fill out appropriate values
cp env.template .env.staging
cp env.template .env.prod
```

#### Deploy

```
# Environment can be passed.  Defaults to "dev"
./setup.sh
./setup.sh master
./setup.sh prod
```

#### Deploy RxNorm Loader
The RxNorm Loader is built with Talend.  Unfortunately, Talend Open Source does not
allow you to build from the command line.  Use the [Talend](https://github.com/rxvantage/data-importer-talend) repo to check out
the code locally and build the project in Talend.  Copy the resulting zip (ie: RxNorm_Loader_0.1.zip) to the `talend/dist` folder.
You can then deploy directly to the environment's S3 bucket.
```
# setup.sh needs to be run first
./bin/stage_rxnorm_loader_to_s3.sh
```

This file does not need to be redeployed unless it is changed, or if the stack is destroyed.

#### Building and Deploying the NPI import script

This repo also contains the NPI importer script. It is built and deployed through `setup.sh`, but can also be built separately:

```bash
python setup.py sdist
./bin/stage_runner_to_s3.sh staging
```

#### Destroy a stack

This will delete all components, _including the S3 bucket_ and any staged files!:

```bash
# Environment can be passed.  Defaults to "dev"
./teardown.sh
./teardown.sh master
./teardown.sh prod
```

#### Usage

The Lambda functions are cron'd, but can also be run manually in [AWS](https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions).

Configure an event, for example:

```json
{
  "period": "weekly",
  "debug": true
}
```

Input parameters for the various functions can be seen under `serverless/`.

_Note that the Cloudwatch events used to trigger the functions are created manually, outside of the Cloudformation stack._

## Overview

#### Packaging

The directory structure is laid out as follows:

```
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
serverless
└── ...function schedules and configuration variables...
├── runner-import.py              # Runner for the NPI importer
├── serverless.yml                # Controls Lamba deployment and AWS resources
└── setup.py                      # Build the python importer
```

#### NPI Importer

NPI data is loaded using the `runner-import.py` script. It is automatically staged to the S3 bucket during deployment, and downloaded by the EC2 instance.

The lambdas live under `lamdbas/npi/`

See: https://cloudcraft.co/view/a49965c0-e2ef-4819-99c1-03722a3ce26e?key=JROwMt9RjsrSxSYUovuIqw

#### Products Importer

The Products data is loaded using the [Talend loader](https://github.com/rxvantage/data-importer-talend). It uses the RxNorm data set.

The Talend loader is a jar built in Talend, and kept in a separate repo. It must be staged to the S3 script bucket manually, with the name `RxNorm_Loader.zip`.

The lambdas live under `lambdas/products/`

#### Database backup

The database backup runs a `mysqldump` to backup the database.

The lambdas live under `lambdas/db_backup/`

## Add a new Lambda

You can add a new lambda with a few steps. The lambdas are responsible for launching an EC2 with any necessary environment variables. The userdata scripts are responsible for doing your work, terminating the instance, and sending any notifications.

1. Create a new folder under `lambdas` and add a handler (Python function). This is responsible for launching the EC2 with your custom userdata script. You can copy from an existing folder, like `lambdas/mysql_backup/`
1. Add a new "body" userdata script under `lambdas/resources/userdata/`. This is shell code which should perform your task.
   - The `start.sh` and `finish.sh` scripts should be concatenated around your "body" script to ensure the EC2 is terminated properly. This is done inside the lambda.
1. Add the handler (function) name to `serverless.yaml`.
1. Add any needed non-secret configuration data under the `serverless/` directory
1. Deploy: `./setup.sh <env>`

The `start.sh` and `finish.sh` are used as wrappers around your body data script. They ensure your EC2 is terminated, and
alerts are sent to SNS (if desired). They also handle setting up environment variables for database access. _Be sure to use them or your instance will not terminate properly!_

## Dependencies

- The lambdas have a dependency on the sql resources in the importer, under `importer/sql/`
- AWS subnets and security groups need to be created ahead of time
