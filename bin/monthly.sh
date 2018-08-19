#!/bin/bash

#PREFIX=${1}
PREFIX="npi-in/monthly"
STAGE=${2:-dev}

if [ -n "$PREFIX" ]; then
    echo "LOADING $PREFIX..."
    sls --stage=$STAGE invoke --function import_npi_monthly --data '{"Records": [{ "s3": {"object": { "PREFIX": "'$PREFIX'"}, "bucket": { "name": "'$(./bin/get_bucket_name.sh)'"}}}]}'
else
    echo "Loading all weekly files..."
    #sls --stage=$STAGE invoke --function import_npi_weekly
fi
