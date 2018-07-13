#!/bin/bash
set -eu

STAGE="${1:-dev}"
APP=importer

BUCKET_NAME=$(aws \
    cloudformation describe-stacks \
    --stack-name "${APP}-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='ScriptBucket'] | [0].OutputValue" \
    --output text)

echo "${BUCKET_NAME}"

aws s3 cp requirements.txt s3://${BUCKET_NAME}/importer/
aws s3 cp runner-import.py s3://${BUCKET_NAME}/importer/
aws s3 cp loaders s3://${BUCKET_NAME}/importer/loaders/ --recursive
