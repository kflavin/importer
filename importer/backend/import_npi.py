import os
import boto3
import botocore
from pprint import pprint
from importer.loaders.npi import NpiLoader
from pprint import pprint


def handler(event, context):
    print("Import NPI data")
    filename = event['filename']
    batch_size = event.get('batch_size', 1000)

    infile = download_csv(filename)

    npi_loader = NpiLoader(user=os.environ['db_user'], host=os.environ['db_host'], password=os.environ['db_password'], database=os.environ['db_schema'], table_name="kyle_npi")
    npi_loader.create_table()
    npi_loader.load(infile, batch_size=batch_size)
    
    print("Done!")


def download_csv(filename):
    bucket_name = os.environ.get('s3_bucket')
    file_full_path = "/tmp/{}".format(filename)
    
    s3 = boto3.resource('s3')
    
    try:
        print("Downloading file to {}...".format(file_full_path))
        s3.Bucket(bucket_name).download_file(filename, file_full_path)
        print("File Downloaded")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    return file_full_path
    