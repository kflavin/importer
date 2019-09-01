#!/bin/bash -e
###################################################################################################################################
# Deploy stack.  Use ./teardown.sh <stage> to remove stack.
#  This will do the following:
#   - Set the correct environmet
#   - Deploy the CF stack
#   - set SSM parameters
#
#  Usage: ./setup.sh <stage>
###################################################################################################################################

set +e
git diff-index --quiet HEAD
if [[ $? -ne 0 ]]; then
    echo "Git working directory is dirty.  Please checkout a clean branch before deploying."
    exit 1
fi
set -e

STAGE=$(echo ${1:-dev} | tr '[:upper:]' '[:lower:]')
ENV_FILE=.env.${STAGE}
echo "Deploying to $STAGE using $ENV_FILE"

source $ENV_FILE

sls deploy --stage=$STAGE

# Load parameters into SSM
bin/set_ssm_params.sh $STAGE

echo "Configured to deploy to: ${db_host}, schema: ${db_schema}"
