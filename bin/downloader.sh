#!/bin/bash

FILE_URL=${1}
STAGE=${2:-dev}

if [ -n "$FILE_URL" ]; then
    echo "Downloading $FILE_URL..."
    sls --stage=$STAGE invoke --function npi_downloader --data '{ "file_url": "'$FILE_URL'" }'
else
    echo "Download all available files..."
    sls --stage=$STAGE invoke --function npi_downloader
fi
