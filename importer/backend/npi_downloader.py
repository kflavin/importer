import boto3
import shutil
import os
from urllib.request import urlopen
from zipfile import ZipFile

def handler(event, context):
    print("start")
    zip = urlopen("http://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_071018.zip")

    s3 = boto3.resource('s3')
    obj = s3.Object(os.environ.get('s3_bucket'), event.get('infile'))
    body = obj.get()['Body']
    s3_upload = boto3.client('s3')
    s3_upload.upload_fileobj(body, os.environ.get('s3_bucket'), event.get('outfile'))
    print("Done!")

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