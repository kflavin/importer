#!/bin/bash
set -eu

STAGE="${1:-dev}"
APP=importer

echo aws cloudformation describe-stacks \
         --stack-name "${APP}-${STAGE}" \
         --query "Stacks[0].Outputs[?OutputKey=='DbBackupBucket'] | [0].OutputValue" \
         --output text

BUCKET_NAME=$(aws \
    cloudformation describe-stacks \
    --stack-name "${APP}-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='DbBackupBucket'] | [0].OutputValue" \
    --output text)

echo "${BUCKET_NAME}"


