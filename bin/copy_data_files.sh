#!/bin/bash
set -eu

DATA_FILE=$1
STAGE="${2:-dev}"

BUCKET_NAME=$(aws \
    cloudformation describe-stacks \
    --stack-name "importer-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='ScriptBucket'] | [0].OutputValue" \
    --output text)

echo "Copying $1 to ${BUCKET_NAME}"

aws s3 cp "${DATA_FILE}" "s3://${BUCKET_NAME}/"

