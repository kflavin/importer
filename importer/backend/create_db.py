import os
import boto3
import botocore
from pprint import pprint
from importer.loaders.npi import NpiLoader
from pprint import pprint


def handler(event, context):
    table_name = event.get('table_name', 'kyle_npi')

    print("Creating database '{os.environ['db_schema']}' and table '{table_name}'...")
    npi_loader = NpiLoader(user=os.environ['db_user'],
                            host=os.environ['db_host'],
                            password=os.environ['db_password'], 
                            database=os.environ['db_schema'],
                            table_name=table_name,
                            set_db=False)
    npi_loader.create_database()
    npi_loader.create_table()
    
    print("Done!")


    