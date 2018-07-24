#!/bin/bash -e

sls deploy --stage=dev
sls invoke --function create_db

# Replace the db and setup params
sed -i -e 's/^export db_host.*/export db_host="'$(./bin/get_rds_endpoint.sh)'"/' .env.aws
source .env.aws
bin/set_ssm_params.sh

# Copy sample data
bin/copy_data_files.sh data/npidata_pfile_1k.csv
bin/copy_data_files.sh data/npidata_pfile_10k.csv
bin/copy_data_files.sh data/npidata_pfile_20k.csv

# Stage package
python setup.py sdist
bin/stage_runner_to_s3.sh
