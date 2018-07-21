#!/bin/bash

bin/delete_bucket_files.sh
sls remove --stage=dev
