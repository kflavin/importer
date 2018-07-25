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
full_dir = "npi-in/full"

def handler(event, context):
    print("Downloading zip files")
    urls = event.get('infile') if event.get('infile', '') else find_zip_urls()

    # urls = find_zip_urls()
    bucket = os.environ.get('aws_s3_bucket')
    for url in urls:
        print(f"Loading {url}")
        url_to_s3(url, bucket)
        
    print("Done!")

def url_to_s3(url, bucket):
    # zip = urlopen("http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_071018.zip")
    # zippedFile = urlopen("http://download.cms.gov/nppes/NPPES_Data_Dissemination_070218_070818_Weekly.zip")
    zippedFile = urlopen(url)
    fileName = url.split("/")[-1]
    # outfile = "npi-in/NPPES_Data_Dissemination_070218_070818_Weekly.zip"

    s3_upload = boto3.client('s3')

    if "weekly" in fileName.lower():
        key = f"{weekly_dir}/{fileName}"
    else:
        key = f"{full_dir}/{fileName}"

    # If it's already in our bucket, skip it.
    if not exists(bucket, key):
        print(f"uploading...")
        s3_upload.upload_fileobj(zippedFile, bucket, key)

# def s3_to_s3(event):
#     """
#     Testing...
#     """
#     s3 = boto3.resource('s3')
#     obj = s3.Object(os.environ.get('aws_s3_bucket'), event.get('infile'))
#     body = obj.get()['Body']
#     s3_upload = boto3.client('s3')
#     s3_upload.upload_fileobj(body, os.environ.get('aws_s3_bucket'), event.get('outfile'))

# if __name__ == "__main__":
#     output = "output"

#     # zip = urlopen("http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_071018.zip")
#     # zip = urlopen("http://download.cms.gov/nppes/NPPES_Data_Dissemination_070218_070818_Weekly.zip")
#     fileName = "NPPES_Data_Dissemination_070218_070818_Weekly.zip"

#     try:
#         shutil.rmtree(output)
#     except:
#         pass
#     finally:
#         os.mkdir(output)

#     # http://download.cms.gov/nppes/NPPES_Data_Dissemination_070218_070818_Weekly.zip
#     zip = urlopen(f"http://download.cms.gov/nppes/{fileName}")
#     with open(f"./{output}/{fileName}", "wb") as f:
#         f.write(zip.read())

#     zipFile = ZipFile(f"{output}/{fileName}")
#     # import pdb; pdb.set_trace()

#     # zipFile = ZipFile(zip.read())
#     zipFile.extractall(f"./{output}")

def exists(bucket, key):
    s3 = boto3.resource('s3')
    print(f"check if {bucket}/{key} exists in s3")

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
    html = urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    # links = soup.find_all('a', href=True)
    links = soup.select("a[href*=.zip]")

    urls = []
    for l in links:
        link = l['href']
        print(f"Checking link {link}")
        if "nppes_data_dissemination" in link.lower():
            urls.append(urljoin(base_url, link))

    return urls

if __name__ == "__main__":
    from bs4 import BeautifulSoup
    html = urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    # links = soup.find_all('a', href=True)
    links = soup.select("a[href*=.zip]")
    print(len(links))

    for link in links:
        # print(link)
        print(link)

