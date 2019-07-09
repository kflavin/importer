#!/bin/bash
#
# Calls the Talend job responsible for loading rxnorm product data.  Handles sending out notifications to SNS.
#

##################
# Set Values
##################
LOADER_ENVIRONMENT=${1:-"dev-rxnorm"}  # dev|stage|prod - These must match values in AWS parameter store.
REGION="us-east-1"
LOG_FILE="/home/ec2-user/jobs/logs/ProductLoader.log"

if [[ "$LOADER_ENVIRONMENT" == "prod" ]]; then
    SNS_ARN="arn:aws:sns:us-east-1:128161413316:data-importer-prod"
else
    SNS_ARN="arn:aws:sns:us-east-1:128161413316:data-importer-dev"
fi

##################
# Run the import
##################

# Make sure we only run one at a time...
run_count=$(ps -ef | grep -c '[j]ava')
if [[ run_count -gt 0 ]]; then
    echo "Multiple java processes running!  Exiting..." 2>&1 | tee -a $LOG_FILE
    exit 1
fi

INSTANCE_ID=$(curl -q http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)

# On completion, send a status update to SNS
function cleanup {
  EXIT_CODE=$?
  set +e  # Do not exit immediately on error

  # Calculate run time
  end=$(date +%s)
  total_time=$((end-start))

  logger "RxNorm import completed. code=$EXIT_CODE, line=$1"

  # Send status to SNS
  if [[ $EXIT_CODE -ne 0 ]]; then
    RESULT="failed"
  else
    RESULT="succeeded"
  fi

  aws --region "$REGION" sns publish --topic-arn "$SNS_ARN" --subject "RxNorm import for $LOADER_ENVIRONMENT $RESULT." --message "RxNorm $LOADER_ENVIRONMENT importer $RESULT on $INSTANCE_ID after $total_time seconds."
}
trap 'cleanup $LINENO' EXIT

set -e
export start=$(date +%s)
export loader_db_host=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_host" --region "$REGION" --with-decryption --query Parameters[0].Value --output text)
export loader_db_user=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_user" --region "$REGION" --with-decryption --query Parameters[0].Value --output text)
export loader_db_password=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_password" --region "$REGION" --with-decryption --query Parameters[0].Value --output text)
export loader_db_schema=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/db_schema" --region "$REGION" --with-decryption --query Parameters[0].Value --output text)
export loader_stage_db_schema=$(aws ssm get-parameters --names "/importer/$LOADER_ENVIRONMENT/stage_db_schema" --region "$REGION" --with-decryption --query Parameters[0].Value --output text)
cd /home/ec2-user/jobs/RxNorm_Loader

## Load stage tables, and call SP
time bash RxNorm_Loader_run.sh --context_param contextName="$LOADER_ENVIRONMENT" 2>&1 | tee -a $LOG_FILE

## Only call the SP's (don't load the stage tables)
#time bash RxNorm_Loader_run.sh --context_param contextName="$LOADER_ENVIRONMENT" --context_param startAt=sp 2>&1 | tee -a /home/ec2-user/jobs/logs/LoadProductData.log


