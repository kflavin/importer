#!/bin/bash -e
###################################################################################################################################
# Run for the FIRST stack deployment.  Use bin/deploy.sh <stage> for subsequent builds.  Use ./teardown.sh <stage> to remove stack.
#  This will do the following:
#   - Deploy the CF stack
#   - set SSM parameters
#
#  For "dev" builds ONLY it will also:
#   - update your .env file with db_host of the RDS instance
#
#  Usage: ./setup.sh <stage>
###################################################################################################################################

set +e
git diff-index --quiet HEAD
if [[ $? -ne 0 ]]; then
    echo "Git working directory is dirty.  Please checkout a clean branch before deploying."
    exit 1
fi
set -e

STAGE=$(echo ${1:-dev} | tr '[:upper:]' '[:lower:]')
ENV_FILE=.env.${STAGE}
echo "Deploying to $STAGE using $ENV_FILE"

source $ENV_FILE

if [[ $STAGE == "dev" ]]; then
  cp serverless-template-dev.yml serverless.yml
else
  cp serverless-template.yml serverless.yml
fi

sls deploy --stage=$STAGE

# Replace the db and setup params (dev only)
if [[ $STAGE == "dev" ]]; then
  grep db_host $ENV_FILE
  sed -i -e 's/^export db_host.*/export db_host="'$(./bin/get_rds_endpoint.sh $STAGE)'"/' $ENV_FILE
  grep db_host $ENV_FILE
  ./bin/get_rds_endpoint.sh $STAGE
  source $ENV_FILE  # source again to get DB host name
  #sls invoke --stage=$STAGE --function create_db --data '{ "table_name": "'$npi_table_name'", "database": "'$db_schema'", "log_table_name": "'$npi_log_table_name'" }'
fi

# Load parameters into SSM
bin/set_ssm_params.sh $STAGE

# Build and stage the runner to S3
#python setup.py sdist
#bin/stage_runner_to_s3.sh $STAGE

echo
echo "-----------------------------------------"
echo "Before starting, source the new env file:"
echo "source $ENV_FILE"
echo "-----------------------------------------"
