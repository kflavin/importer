#!/bin/bash -e

STAGE="${1:-dev}"
ENV_FILE=${2:.env.aws}

sls deploy --stage=$STAGE

# Replace the db and setup params
sed -i -e 's/^export db_host.*/export db_host="'$(./bin/get_rds_endpoint.sh $STAGE)'"/' $ENV_FILE
source .env.aws
bin/set_ssm_params.sh $STAGE
# NAT gateway isn't connected to subnet at this point
#sls invoke --stage=$STAGE --function create_db --data '{ "table_name": "'$npi_table_name'", "database": "'$db_schema'", "log_table_name": "'$npi_log_table_name'" }'

# Copy sample data
#bin/copy_data_files.sh data/npidata_pfile_1k.csv
#bin/copy_data_files.sh data/npidata_pfile_10k.csv
#bin/copy_data_files.sh data/npidata_pfile_20k.csv

# Stage package
python setup.py sdist
bin/stage_runner_to_s3.sh $STAGE
