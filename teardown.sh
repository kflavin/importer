#!/bin/bash

STAGE="${1:-dev}"

bin/delete_bucket_files.sh $STAGE
sls remove --stage=$STAGE
