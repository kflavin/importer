#!/bin/bash

aws ssm put-parameter --name "db_host" --type "String" --value "${db_host}" --overwrite
aws ssm put-parameter --name "db_user" --type "SecureString" --value "${db_user}" --overwrite
aws ssm put-parameter --name "db_password" --type "SecureString" --value "${db_password}" --overwrite
aws ssm put-parameter --name "db_schema" --type "SecureString" --value "${db_schema}" --overwrite

