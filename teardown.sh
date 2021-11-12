#!/bin/bash

STAGE="${1:-dev}"
source .env.${STAGE}
echo "Tear down ${STAGE} with .env.${STAGE}"

bin/delete_bucket_files.sh $STAGE
sls remove --stage=$STAGE

aws ssm delete-parameter --name /importer/${STAGE}/db_host
aws ssm delete-parameter --name /importer/${STAGE}/db_user
aws ssm delete-parameter --name /importer/${STAGE}/db_password
aws ssm delete-parameter --name /importer/${STAGE}/db_name
aws ssm delete-parameter --name /importer/${STAGE}/db_schema
aws ssm delete-parameter --name /importer/${STAGE}/stage_db_schema
