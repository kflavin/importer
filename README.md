# serverless importer

This repo is used to dynamically launch EC2 instances for data loading.  There are two distinct parts:

* Lambda functions, under `lambda/`, which handle the loading
* An importer script, which is only used for NPI data, under `importer/`

It currently loads the following data:

* Loading the `npis` table
* Loading the RxNorm product data
* Database backups

## Installation and Usage

Each loader gets its own AWS Lambda function.

#### Pre-reqs

To deploy the Lambda functions, you need the following:

* python3
* You should have your AWS creds file configured.  If you are using profiles, be sure to set your profile name in your environment file.

#### Building and Deploying the Lambda functions

Each environment corresponds to a [Cloudformation stack](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false).:

Currently deployed stacks are `rc`, `stage`, and `prod`.

```bash
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

This repo also contains the NPI importer script.  It is built and deployed through `setup.sh`, but can be built separately:

```bash
python setup.py sdist
./bin/stage_runner_to_s3.sh rc
```

#### Destroy a stack

This will delete all components, _including the S3 bucket_:

```bash
./teardown.sh rc
./teardown.sh stage
./teardown.sh prod
```

#### Usage

The Lambda functions are cron'd, but can also be run manually in [AWS](https://console.aws.amazon.com/lambda/home?region=us-east-1#/functions).

To run, configure an empty test event (event isn't used, all parameters are passed through the environment).

## Overview

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
packer
└── ...EC2 AMI...
resources
└── ...AWS resources...
├── runner-import.py              # Runner for the NPI importer
├── serverless-template.yml       # Controls Lamba deployment and AWS resources
└── setup.py                      # Build the python importer
```

#### NPI Importer

NPI data is loaded using the `importer/` script.  It is staged to the S3 bucket during deployment, and downloaded by the EC2 instance.

The lambdas live under `lamdbas/npi/`

See: https://cloudcraft.co/view/a49965c0-e2ef-4819-99c1-03722a3ce26e?key=JROwMt9RjsrSxSYUovuIqw

#### RxNorm Importer

The RxNORM data is loaded using the [Talend loader](https://github.com/rxvantage/data-importer-talend)

The lambdas live under `lambdas\product`

#### Database backup

The lambdas live under `lambdas\db_backup`

### Dependencies

* The lambdas have a dependency on the sql resources in the importer, under `importer/sql/`
* The importer script has a dependency on AWS for cloudwatch logging
* AWS subnets and security groups need to be created ahead of time

