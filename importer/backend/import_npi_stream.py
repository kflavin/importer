import os
import boto3
from importer.loaders.npi import NpiLoader
import io

class S3ObjectInterator(io.RawIOBase):
    def __init__(self, bucket, key):
        """Initialize with S3 bucket and key names"""
        self.s3c = boto3.client('s3')
        self.obj_stream = self.s3c.get_object(Bucket=bucket, Key=key)['Body']

    def read(self, n=-1):
        """Read from the stream"""
        return self.obj_stream.read() if n == -1 else self.obj_stream.read(n)

def handler(event, context):
    print("Print streaming data")

    obj_stream = S3ObjectInterator(os.environ.get('s3_bucket'), event.get('filename'))
    for line in obj_stream:
        print(line)

    print("Done!")
