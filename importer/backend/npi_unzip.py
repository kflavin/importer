import boto3
import os
# import zipfile
# import io

def handler(event, context):
    print("start")
    s3 = boto3.resource('s3')
    obj = s3.Object(os.environ.get('s3_bucket'), event.get('infile'))
    body = obj.get()['Body']
    s3_upload = boto3.client('s3')
    s3_upload.upload_fileobj(body, os.environ.get('s3_bucket'), event.get('outfile'))
    print("Done!")
