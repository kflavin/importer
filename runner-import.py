#!/usr/bin/env python3
import click
import os
import boto3
from importer.loaders.npi import NpiLoader
from importer.loggers.cw_logger import CloudWatchLogger

@click.group()
def start():
    pass

@click.group()
def npi():
    pass

@click.command()
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="Table to create")
def create(table_name):
    """
    Create the initial NPI table.
    """
    print(f"Create NPI Table: {table_name}")
    npi_loader = NpiLoader()
    npi_loader.connect(user=os.environ['db_user'], host=os.environ['db_host'], password=os.environ['db_password'], database=os.environ['db_schema'])
    npi_loader.create_table(table_name)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="Table name to load.")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
def load(infile, batch_size, table_name, period):
    """
    NPI importer
    """

    client = boto3.client('ssm', region_name=os.environ['aws_region'])
    # Environment params will override SSM params
    args = {
        'user': os.environ.get('db_user', client.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value']),
        'password': os.environ.get('db_password', client.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value']),
        'host': os.environ.get('db_host', client.get_parameter(Name='db_host')['Parameter']['Value']),
        'database': os.environ.get('db_schema', client.get_parameter(Name='db_schema', WithDecryption=True)['Parameter']['Value'])
    }

    npi_loader = NpiLoader()
    if period.lower() == "weekly":
        print("Loading weekly NPI file")
        npi_loader.connect(**args)
        npi_loader.load_weekly(table_name, infile, batch_size)
    else:
        print("Loading monthly NPI file")
        npi_loader.connect(**args, clientFlags=True)
        npi_loader.load_monthly(table_name, infile)

    print(f"Data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--outfile', '-o', type=click.STRING, help="Filename to write out")
def npi_preprocess(infile, outfile):
    """
    Preprocess NPI csv file to do things like remove extraneous columns
    """
    npi_loader = NpiLoader()
    npi_loader.preprocess(infile, outfile)
    print(outfile)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="File to unzip")
@click.option('--unzip-path', '-u', required=True, type=click.STRING, help="Directory to extract to")
def npi_unzip(infile, unzip_path):
    npi_loader = NpiLoader()
    csv_file = npi_loader.unzip(infile, unzip_path)
    print(csv_file)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="File to unzip")
@click.option('--unzip-path', '-u', default="/data/NPPES", type=click.STRING, help="Directory to extract zip")
@click.option('--outfile', '-o', type=click.STRING, help="Filename to write for cleaned file")
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="Table name to load.")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--bucket-name', type=click.STRING, help="")
@click.option('--bucket-key', type=click.STRING, help="")
def all(infile, unzip_path, outfile, batch_size, table_name, period, bucket_name, bucket_key):
    """
    Perform all steps.  Assumes we are loading to AWS.
    """
    # Trying to keep AWS imports isolated here for now...
    from importer.loggers.cw_logger import CloudWatchLogger
    from importer.loaders.npi import NpiLoader
    import boto3
    logger.info(f"Processing {period} file")

    region = os.environ.get('aws_region')
    logger = CloudWatchLogger("importer-npi", os.environ.get('instance_id'), region=region)

    npi_loader = NpiLoader()

    logger.info("Unzipping file")
    csv_file = npi_loader.unzip(infile, unzip_path)

    logger.info("Preprocessing file")
    cleaned_file = npi_loader.preprocess(csv_file, outfile)

    ssm = boto3.client('ssm', region_name=region)

    args = {
        'user': os.environ.get('db_user', ssm.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value']),
        'password': os.environ.get('db_password', ssm.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value']),
        'host': os.environ.get('db_host', ssm.get_parameter(Name='db_host')['Parameter']['Value']),
        'database': os.environ.get('db_schema', ssm.get_parameter(Name='db_schema', WithDecryption=True)['Parameter']['Value'])
    }

    npi_loader.connect(**args)
    
    if period.lower() == "weekly":
        logger.info("Loading weekly NPI file into database")
        npi_loader.load_weekly(table_name, cleaned_file, batch_size)
    else:
        logger.info("Loading monthly NPI file into database")
        # npi_loader.disable_checks()
        npi_loader.load_monthly(table_name, cleaned_file)
        # npi_loader.enable_checks()

    logger.info("Mark file as imported in S3")
    npi_loader.mark_imported(bucket_name, bucket_key)
    
    logger.info(f"Data loaded to table: {table_name}")


start.add_command(npi)
npi.add_command(load)
npi.add_command(create)
npi.add_command(npi_unzip, name="unzip")
npi.add_command(npi_preprocess, name="preprocess")
npi.add_command(all)

if __name__ == '__main__':
    start()

