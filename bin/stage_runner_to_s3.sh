#!/bin/bash
set -eu

STAGE="${1:-dev}"
APP=importer

BUCKET_NAME=$(aws \
    cloudformation describe-stacks \
    --stack-name "${APP}-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='ScriptBucket'] | [0].OutputValue" \
    --output text)

# aws s3 cp requirements.txt s3://${BUCKET_NAME}/lib/
# aws s3 cp runner-import.py s3://${BUCKET_NAME}/lib/
# aws s3 cp importer s3://${BUCKET_NAME}/lib/importer/ --recursive
echo "----- Uploading runner to stage: $STAGE -----"
aws s3 cp dist/*.tar.gz s3://${BUCKET_NAME}/importer.tar.gz
