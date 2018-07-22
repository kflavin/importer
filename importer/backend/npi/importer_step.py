import os
import boto3
import botocore
from importer.loaders.npi import NpiLoader

def handler(event, context):
    print("Import NPI data")

    filename = event['filename']
    infile = download_csv(filename)
    chunk_size = event.get('chunk_size', 1000)

    results = event.get('results', {"processedRows": 0, "finished": False })
    # Sum all rows in the file on first run only
    total = results.get('total') if results.get('total') else sum(1 for _ in open(infile, 'rb'))

    start = results.get('processedRows')
    if start == 0:
        start += 1  # skip headers
    end = start + chunk_size

    print("Processing chunk: start={}, end={}".format(start, end))

    npi_loader = NpiLoader(user=os.environ['db_user'], host=os.environ['db_host'], password=os.environ['db_password'], database=os.environ['db_schema'], table_name="kyle_npi")
    npi_loader.step_load(infile, start, end)
        
    results['processedRows'] += chunk_size

    if results['processedRows'] >= total:
        results['finished'] = True
    
    print("Done!")
    return {"results": results, "code": 0, "filename": filename, "chunk_size": chunk_size}


def download_csv(filename):
    bucket_name = os.environ.get('aws_s3_bucket')
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
    