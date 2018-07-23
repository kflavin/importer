#!/bin/bash -e

sls deploy --stage=dev
sls invoke --function create_db
#bin/set_ssm_params.sh

bin/copy_data_files.sh data/npidata_pfile_1k.csv
bin/copy_data_files.sh data/npidata_pfile_10k.csv
bin/copy_data_files.sh data/npidata_pfile_20k.csv

# Stage package
python setup.py sdist
bin/stage_runner_to_s3.sh
