#!/bin/bash

STAGE=${1:-dev}
shift

if [ -e "serverless.yml" ]; then
    rm -f serverless.yml
fi

#ln -s "serverless-$STAGE.yml" serverless.yml
if [ ! -f "serverless-$STAGE.yml" ]; then
    echo "No serverless file exists for \"$STAGE\".  Please create serverless-$STAGE.yml"
    exit 1
fi
cp "serverless-$STAGE.yml" serverless.yml

if [ ! -f ".env.$STAGE" ]; then
    echo "No environment file exists for $STAGE.  Please create .env.$STAGE"
    exit 1
fi

echo "----- Source .env.$STAGE -----"
source .env.$STAGE

echo "----- Build Runner -----"
python setup.py sdist

echo "----- Deploying to $STAGE using serverless-$STAGE.yml -----"
sls deploy --stage=$STAGE $*
