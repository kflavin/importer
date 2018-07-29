import boto3
import botocore
import shutil
import os
from urllib.request import urlopen
from urllib.parse import urljoin
from zipfile import ZipFile
from bs4 import BeautifulSoup

url = "http://download.cms.gov/nppes/NPI_Files.html"
base_url = "http://download.cms.gov/nppes/"
weekly_dir = "npi-in/weekly"
monthly_dir = "npi-in/monthly"

def handler(event, context):
    """
    Download zip files.  Don't download if they already exist in s3.
    """
    print("Downloading zip files")
    # If a file is not passed as a param, then crawl the page for zip files.
    urls = [event.get('file_url')] if event.get('file_url', '') else find_zip_urls()
    bucket = os.environ.get('aws_s3_bucket')

    for url in urls:
        url_to_s3(url, bucket)

    print("Done!")

def url_to_s3(url, bucket):
    """
    Download the zip file to S3
    """
    zippedFile = urlopen(url)
    fileName = url.split("/")[-1]
    client = boto3.client('s3')

    if "weekly" in fileName.lower():
        key = f"{weekly_dir}/{fileName}"
    else:
        key = f"{monthly_dir}/{fileName}"

    # If it's already in our bucket, skip it.
    if not exists(bucket, key):
        print(f"Uploading {bucket}/{key}")
        client.upload_fileobj(zippedFile, bucket, key)

        # Tag the object
        client.put_object_tagging(
            Bucket=bucket,
            Key=key,
            # VersionId='string',
            # ContentMD5='string',
            Tagging={
                'TagSet': [
                    {
                        'Key': 'imported',
                        'Value': 'false'
                    },
                ] } )
    else:
        print(f"Skipping {bucket}/{key}, exists.")

# def s3_to_s3(event):
#     """
#     Testing...
#     """
#     s3 = boto3.resource('s3')
#     obj = s3.Object(os.environ.get('aws_s3_bucket'), event.get('infile'))
#     body = obj.get()['Body']
#     s3_upload = boto3.client('s3')
#     s3_upload.upload_fileobj(body, os.environ.get('aws_s3_bucket'), event.get('outfile'))

def exists(bucket, key):
    """
    Check if the object exists in s3
    """
    s3 = boto3.resource('s3')
    # print(f"check if {bucket}/{key} exists in s3")

    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            print("Unknown error.")
            raise
    
    return True


def find_zip_urls():
    """
    Locate any zip files on the site and return them.
    """
    html = urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.select("a[href*=.zip]")

    urls = []
    for l in links:
        link = l['href']

        if "nppes_data_dissemination" in link.lower():
            print(f"Adding link {link}")
            urls.append(urljoin(base_url, link))

    return urls

if __name__ == "__main__":
    """
    Test from the CLI
    """
    html = urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    # links = soup.find_all('a', href=True)
    links = soup.select("a[href*=.zip]")
    print(len(links))

    for link in links:
        print(link)

