#!/bin/bash

STAGE="${1:-dev}"
source .env.${STAGE}
echo "Tear down ${STAGE} with .env.${STAGE}"

bin/delete_bucket_files.sh $STAGE
sls remove --stage=$STAGE
