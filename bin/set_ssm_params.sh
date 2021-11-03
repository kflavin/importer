#!/bin/bash

STAGE=${1:-dev}

AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/db_host" --type "String" --value "${db_host}" --overwrite
AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/db_user" --type "SecureString" --value "${db_user}" --overwrite
AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/db_password" --type "SecureString" --value "${db_password}" --overwrite
AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/db_name" --type "SecureString" --value "${db_name}" --overwrite
AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/stage_db_name" --type "SecureString" --value "${stage_db_name}" --overwrite


# Must set use_replica=1 on the lambda environment variable
if [[ ! -z ${db_host_replica+x} ]]; then
    echo "Set replica"
    AWS_PAGER="" aws ssm put-parameter --name "/importer/${STAGE}/db_host_replica" --type "SecureString" --value "${db_host_replica}" --overwrite
fi

