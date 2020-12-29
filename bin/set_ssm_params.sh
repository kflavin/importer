#!/bin/bash

STAGE=${1:-dev}

aws ssm put-parameter --name "/importer/${STAGE}/db_host" --type "String" --value "${db_host}" --overwrite
aws ssm put-parameter --name "/importer/${STAGE}/db_user" --type "SecureString" --value "${db_user}" --overwrite
aws ssm put-parameter --name "/importer/${STAGE}/db_password" --type "SecureString" --value "${db_password}" --overwrite
aws ssm put-parameter --name "/importer/${STAGE}/db_schema" --type "SecureString" --value "${db_schema}" --overwrite
aws ssm put-parameter --name "/importer/${STAGE}/stage_db_schema" --type "SecureString" --value "${stage_db_schema}" --overwrite


# Must set use_replica=1 on the lambda environment variable
if [[ ! -z ${db_host_replica+x} ]]; then
    echo "Set replica"
    AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/db_host_replica" --type "SecureString" --value "${db_host_replica}" --overwrite
fi

