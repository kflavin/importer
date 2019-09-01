#!/bin/bash
###################################################################################################################################
# Deprecated.  Use setup.sh now.
#
# Run for deployments.  Use setup.sh to deploy the stack for the first time, and this script after
#
# Usage: bin/deploy.sh <stage>
###################################################################################################################################

STAGE=${1:-dev}
shift

if [ -e "serverless.yml" ]; then
    rm -f serverless.yml
fi

#ln -s "serverless-template-$STAGE.yml" serverless.yml
if [ ! -f "serverless-template-$STAGE.yml" ]; then
    echo "No serverless file exists for \"$STAGE\".  Please create serverless-template-$STAGE.yml"
    exit 1
fi
cp "serverless-template-$STAGE.yml" serverless.yml

if [ ! -f ".env.$STAGE" ]; then
    echo "No environment file exists for $STAGE.  Please create .env.$STAGE"
    exit 1
fi

echo "----- Source .env.$STAGE -----"
source .env.$STAGE

#echo "----- Build Runner -----"
#python setup.py sdist

echo "----- Deploying to $STAGE using serverless-template-$STAGE.yml -----"
sls deploy --stage=$STAGE $*
