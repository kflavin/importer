#!/bin/bash

#PREFIX=${1}
PREFIX="npi-in/weekly"
STAGE=${1:-dev}

if [ -n "$PREFIX" ]; then
    echo "LOADING $PREFIX..."
    sls --stage=$STAGE invoke --function import_npi_weekly --data '{"Records": [{ "s3": {"object": { "PREFIX": "'$PREFIX'"}, "bucket": { "name": "'$(./bin/get_bucket_name.sh $STAGE)'"}}}]}'
else
    echo "Loading all weekly files..."
    #sls --stage=$STAGE invoke --function import_npi_weekly
fi
