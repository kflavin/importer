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
aws s3 cp --recursive config/ s3://${BUCKET_NAME}/config/
echo "----- Build runner -----"
python setup.py sdist
echo "----- Copy runner to ${BUCKET_NAME} -----"
aws s3 cp dist/*.tar.gz s3://${BUCKET_NAME}/importer.tar.gz

