import boto3
import shutil
import os
from urllib.request import urlopen
from zipfile import ZipFile

def handler(event, context):
    print("start")
    url_to_s3(event)
    print("Done!")

def url_to_s3(event):
    # zip = urlopen("http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_071018.zip")
    zippedFile = urlopen("http://download.cms.gov/nppes/NPPES_Data_Dissemination_070218_070818_Weekly.zip")
    outfile = "npi-in/NPPES_Data_Dissemination_070218_070818_Weekly.zip"
    s3_upload = boto3.client('s3')
    s3_upload.upload_fileobj(zippedFile, os.environ.get('aws_s3_bucket'), outfile)

def s3_to_s3(event):
    s3 = boto3.resource('s3')
    obj = s3.Object(os.environ.get('aws_s3_bucket'), event.get('infile'))
    body = obj.get()['Body']
    s3_upload = boto3.client('s3')
    s3_upload.upload_fileobj(body, os.environ.get('aws_s3_bucket'), event.get('outfile'))

if __name__ == "__main__":
    output = "output"

    # zip = urlopen("http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_071018.zip")
    # zip = urlopen("http://download.cms.gov/nppes/NPPES_Data_Dissemination_070218_070818_Weekly.zip")
    fileName = "NPPES_Data_Dissemination_070218_070818_Weekly.zip"

    try:
        shutil.rmtree(output)
    except:
        pass
    finally:
        os.mkdir(output)

    # http://download.cms.gov/nppes/NPPES_Data_Dissemination_070218_070818_Weekly.zip
    zip = urlopen(f"http://download.cms.gov/nppes/{fileName}")
    with open(f"./{output}/{fileName}", "wb") as f:
        f.write(zip.read())

    zipFile = ZipFile(f"{output}/{fileName}")
    # import pdb; pdb.set_trace()

    # zipFile = ZipFile(zip.read())
    zipFile.extractall(f"./{output}")