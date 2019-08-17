#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <file> <stage>"
    exit 1
fi

STAGE="${2:-dev}"
APP=importer

BUCKET_NAME=$(aws \
    cloudformation describe-stacks \
    --stack-name "${APP}-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='ScriptBucket'] | [0].OutputValue" \
    --output text)

echo "${BUCKET_NAME}"
aws s3 cp "$1" s3://${BUCKET_NAME}/


