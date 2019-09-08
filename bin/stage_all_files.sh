#!/bin/bash
set -eu

STAGE="${1:-dev}"
APP=importer

BUCKET_NAME=$(aws \
    cloudformation describe-stacks \
    --stack-name "${APP}-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='ScriptBucket'] | [0].OutputValue" \
    --output text)

echo "----- Copy configs/ to ${BUCKET_NAME} -----"
aws s3 cp --recursive lambdas/resources/config/ s3://${BUCKET_NAME}/config/
echo "----- Build runner -----"
python setup.py sdist
echo "----- Copy runner to ${BUCKET_NAME} -----"
latest_runner_file=$(ls -tr dist/*.tar.gz | tail -n 1)
aws s3 cp $latest_runner_file s3://${BUCKET_NAME}/importer.tar.gz

latest_rxnorm_file=$(ls -tr talend/dist/*.zip | tail -n 1)
aws s3 cp $latest_rxnorm_file s3://${BUCKET_NAME}/RxNorm_Loader.zip
