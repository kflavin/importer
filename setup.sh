#!/bin/bash -e

STAGE=$(echo ${1:-dev} | tr '[:upper:]' '[:lower:]')
ENV_FILE=.env.${STAGE}
echo "Deploying to $STAGE using $ENV_FILE"

if [[ $STAGE == "dev" ]]; then
  cp serverless-dev.yml serverless.yml
else
  cp serverless-prod.yml serverless.yml
fi

sls deploy --stage=$STAGE

# Replace the db and setup params (dev only)
if [[ $STAGE == "dev" ]]; then
  sed -i -e 's/^export db_host.*/export db_host="'$(./bin/get_rds_endpoint.sh $STAGE)'"/' $ENV_FILE
  source $ENV_FILE
  bin/set_ssm_params.sh $STAGE
  sls invoke --stage=$STAGE --function create_db --data '{ "table_name": "'$npi_table_name'", "database": "'$db_schema'", "log_table_name": "'$npi_log_table_name'" }'
fi

# Copy sample data
#bin/copy_data_files.sh data/npidata_pfile_1k.csv
#bin/copy_data_files.sh data/npidata_pfile_10k.csv
#bin/copy_data_files.sh data/npidata_pfile_20k.csv

# Build and stage the runner to S3
python setup.py sdist
bin/stage_runner_to_s3.sh $STAGE

echo "Source the new env file:"
echo "source $ENV_FILE"
