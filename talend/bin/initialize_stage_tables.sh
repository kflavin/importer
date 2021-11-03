#!/bin/bash

# This script is only to be run once.  It loads staging tables in both databases, so the intiialize sp can migrate the product table.

LOADER_ENVIRONMENT=${1:-"dev-rxnorm"}  # dev|stage|prod - These must match values in AWS parameter store.

export loader_db_host=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_host" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export loader_db_user=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_user" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
export loader_db_password=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_password" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
cd /home/ec2-user/jobs/RxNorm_Loader

# Load staging tables to stage-db
export loader_stage_db_name=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/stage_db_name" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
echo "Loading into $loader_db_name"
time bash RxNorm_Loader_run.sh --context_param contextName="$LOADER_ENVIRONMENT" 2>&1 --context_param endAt=sp

# Load staging tables to prod-db
export loader_stage_db_name=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_name" --region us-east-1 --with-decryption --query Parameters[0].Value --output text)
echo "Loading into $loader_db_name"
time bash RxNorm_Loader_run.sh --context_param contextName="$LOADER_ENVIRONMENT" 2>&1 --context_param endAt=sp
