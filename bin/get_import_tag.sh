#!/bin/bash

KEYS=$(aws s3api list-objects --bucket $(./bin/get_bucket_name.sh) --output text | grep CONTENTS | awk '{print $3}')
BUCKET=$(./bin/get_bucket_name.sh)

for KEY in $KEYS; do
    if [[ $KEY =~ zip$ ]]; then
        IMPORTED=$(aws s3api get-object-tagging --bucket $BUCKET --key $KEY --query 'TagSet' --output text | cut -f2)
        printf "%-70s %-20s\n" $KEY $IMPORTED
    fi
done
