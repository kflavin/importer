import boto3
import botocore
import os
from urllib.request import urlopen
from urllib.parse import urljoin
from zipfile import ZipFile
from bs4 import BeautifulSoup
from lambdas.helpers.rds import add_to_db

download_url = "http://download.cms.gov/nppes/NPI_Files.html"
base_url = "http://download.cms.gov/nppes/"
weekly_dir = "npi-in/weekly"
monthly_dir = "npi-in/monthly"
max_links = 8  # If we find more zip files than this, exit.  The NPPES site may have changed.

def handler(event, context):
    """
    Download zip files and put them into the appropriate s3 locations
    """
    print("Downloading zip files")
    table_name = os.environ.get('npi_log_table_name', 'npi_import_log')

    # Enumerate the zip files on the page.  If a file is not passed as a param, then
    # crawl the page for zip files.
    urls = [event.get('file_url')] if event.get('file_url', '') else find_zip_urls()
    bucket = os.environ.get('aws_s3_bucket')

    # Add each file into bucket
    for url in urls:
        url_to_s3(url, bucket, table_name) # download the file

    print("Done!")

def url_to_s3(url, bucket, table_name):
    """
    Download the zip file to the appropriate S3 folder.  Skip files that already exist.
    """
    zippedFile = urlopen(url)
    fileName = url.split("/")[-1]
    period = ""
    client = boto3.client('s3')

    if "weekly" in fileName.lower():
        key = f"{weekly_dir}/{fileName}"
        p = 'w'
    else:
        key = f"{monthly_dir}/{fileName}"
        p = 'm'

    # If it's already in our bucket, skip it.
    if not exists(bucket, key):
        print(f"Uploading {bucket}/{key}")
        client.upload_fileobj(zippedFile, bucket, key)
        add_to_db(url, table_name, p) # add the new entry to the database
    else:
        print(f"Skipping {bucket}/{key}, already exists.")

# def s3_to_s3(event):
#     """
#     Testing a multipart upload...
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
    Locate any zip files we want and return them.  This finds only the dissemination files,
    not deactivation.
    """
    html = urlopen(download_url).read()
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.select("a[href$=.zip]")
    urls = []

    print(f"Found {len(links)} links")
    # Sanity check to ensure the page hasn't changed to something unexpected.
    if len(links) > max_links:
        print("Number of links exceeds MAX of {max_links}, exit and check site for changes.")
        return urls

    for l in links:
        link = l['href']

        # Only grab dissemination links, not deactivation.
        if "/nppes_data_dissemination" in link.lower():
            urls.append(urljoin(base_url, link))

    return urls

if __name__ == "__main__":
    """
    Test from the CLI
    """
    html = urlopen(download_url).read()
    soup = BeautifulSoup(html, 'html.parser')
    # links = soup.find_all('a', href=True)
    links = soup.select("a[href$=.zip]")
    print(len(links))

    for link in links:
        print(link)

