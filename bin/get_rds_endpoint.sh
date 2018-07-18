#!/bin/bash
set -eu

STAGE="${1:-dev}"
APP=importer

RDS_ENDPOINT=$(aws \
    cloudformation describe-stacks \
    --stack-name "${APP}-${STAGE}" \
    --query "Stacks[0].Outputs[?OutputKey=='RdsEndpoint'] | [0].OutputValue" \
    --output text)

echo "${RDS_ENDPOINT}"


