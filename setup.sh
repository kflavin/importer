#!/bin/bash -e
###################################################################################################################################
# Run for the FIRST stack deployment.  Use bin/deploy.sh <stage> for subsequent builds.  Use ./teardown.sh <stage> to remove stack.
#  This will do the following:
#   - Deploy the CF stack
#   - set SSM parameters
#
#  For "dev" builds ONLY it will also:
#   - update your .env file with db_host of the RDS instance
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