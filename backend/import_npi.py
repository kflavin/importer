import os
import boto3
from pprint import pprint
from loaders.npi import NpiLoader
from pprint import pprint


def handler(event, context):
    print("Import NPI data")
    pprint(os.environ)
    # npi_loader = NpiLoader("fakefile", table_name="kyle_npi", batch_size=1000)
    # print(os.path.dirname(os.path.realpath(__file__)))
    # print(os.getcwd())


