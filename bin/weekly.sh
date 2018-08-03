#!/bin/bash

KEY=${1}
STAGE=${2:-dev}

if [ -n "$KEY" ]; then
    echo "LOADING $KEY..."
    sls --stage=$STAGE invoke --function import_npi_weekly --data '{"Records": [{ "s3": {"object": { "key": "'$KEY'"}, "bucket": { "name": "'$(./bin/get_bucket_name.sh)'"}}}]}'
else
    echo "Loading all weekly files..."
    #sls --stage=$STAGE invoke --function import_npi_weekly
fi
