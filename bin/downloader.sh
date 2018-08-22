#!/bin/bash

STAGE=${1:-dev}
FILE_URL=${2}

if [ -n "$FILE_URL" ]; then
    echo "Downloading $FILE_URL..."
    sls --stage=$STAGE invoke --function npi_downloader --data '{ "file_url": "'$FILE_URL'" }'
else
    echo "Download all available files..."
    sls --stage=$STAGE invoke --function npi_downloader
fi
