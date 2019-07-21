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
echo "----- Uploading talend to: $STAGE -----"
latest_file=$(ls -tr talend/dist/*.zip | tail -n 1)
aws s3 cp $latest_file s3://${BUCKET_NAME}/RxNorm_Loader.zip
