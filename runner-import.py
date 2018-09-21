#!/usr/bin/env python3
import click
import os
import logging

from importer.loaders.npi import NpiLoader
from importer.loggers.cloudwatch import CloudWatchLogger

DEBUG = False
logger = None

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--logs', '-l', default="system", type=click.Choice(["cloudwatch", "system"]), help="[cloudwatch|system] - CW requires aws_region/instance_id env vars to be set.")
def start(debug, logs):
    DEBUG = debug
    global logger
    
    if logs == "cloudwatch":
        region = os.environ.get('aws_region')
        logger = CloudWatchLogger("importer-npi", os.environ.get('instance_id'), region=region)
        logger.info("Sending runner logs to cloudwatch")
    else:
        logger = logging.getLogger("importer")
        logger.setLevel(level="INFO")
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)
        # fh = logging.FileHandler("importer.log")
        # fh.setLevel(logging.DEBUG)
        # logger.addHandler(fh)
        logger.info("Sending runner logs to stdout")
    
        

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
@click.option('--url-prefix', '-u', required=True, type=click.STRING, help="URL directory that contains weekly or monthly files to fetch")
@click.option('--table-name', '-t', default="npi_import_log", type=click.STRING, help="Import log table")
@click.option('--period', '-p', required=True, type=click.STRING, help="[weekly|monthly]")
@click.option('--output_dir', '-o', default="/tmp/npi", type=click.STRING, help="Directory to store file on local filesystem")
@click.option('--limit', '-l', default=3, type=click.INT, help="Max # of files to fetch at a time.  Only weekly files are adjustable, monthly is set to 1.")
@click.option('--environment', '-e', default="dev", type=click.STRING, help="User specified environment, ie: dev|rc|stage|prod, etc")
def fetch(url_prefix, table_name, period, output_dir, limit, environment):
    """
    Fetch files from import log table
    """
    print(f"Fetch '{period}' files from {table_name}")
    npi_loader = NpiLoader()
    npi_loader.connect(user=os.environ['db_user'],
                        host=os.environ['db_host'], 
                        password=os.environ['db_password'], 
                        database=os.environ['db_schema'], 
                        dictionary=True, 
                        buffered=True)
    npi_loader.fetch(url_prefix, table_name, period, environment, output_dir, limit)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="Table name to load.")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--large-file', default=False, is_flag=True, help="Use LOAD DATA INFILE instead of INSERT")
def load(infile, batch_size, table_name, period, large_file):
    """
    NPI importer
    """

    args = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema')
    }

    npi_loader = NpiLoader()

    if large_file:
        print(f"Loading {period} (large) file into database.  large_file: {large_file}")
        npi_loader.connect(**args, clientFlags=True)
        npi_loader.disable_checks()     # disable foreign key, unique checks, etc, for better performance
        npi_loader.load_large_file(table_name, infile)
        npi_loader.enable_checks()
    else:
        print(f"Loading {period} (small) file into database.  large_file: {large_file}")
        npi_loader.connect(**args)
        npi_loader.load_file(table_name, infile, batch_size)

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
@click.option('--url-prefix', '-u', required=True, type=click.STRING, help="URL directory that contains weekly or monthly files to fetch")
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="NPI table")
@click.option('--import-table-name', '-i', default="npi_import_log", type=click.STRING, help="NPI import table")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--workspace', '-w', default="/tmp/npi", type=click.STRING, help="Workspace directory")
@click.option('--limit', '-l', default=6, type=click.INT, help="Max # of files to fetch at a time.  Only weekly files are adjustable, monthly is set to 1.")
@click.option('--large-file', default=False, is_flag=True, help="Use LOAD DATA INFILE instead of INSERT")
@click.option('--environment', '-e', default="dev", type=click.STRING, help="User specified environment, ie: dev|rc|stage|prod, etc")
def all(url_prefix, batch_size, table_name, import_table_name, period, workspace, limit, large_file, environment):
    """
    Perform all load steps.
    """
    logger.info(f"Start: {period} file")

    workspace = workspace.rstrip("/")

    args = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema')
    }

    # We don't want to use these for the file loading, just the fetch
    extra_args = {
        'dictionary': True,      # For referencing returned columns by name
        'buffered': True        # so we can get row counts without reading every row
    }

    # Fetch files listed in import log
    logger.info(f"Fetch {period} files from {import_table_name}")
    npi_fetcher = NpiLoader()
    npi_fetcher.connect(**{**args, **extra_args})
    files = npi_fetcher.fetch(url_prefix, import_table_name, period, environment, workspace, limit)
    npi_fetcher.close()

    # Load the files
    logger.info(f"Loading '{period}' files from {table_name}")
    npi_loader = NpiLoader()
    npi_loader.connect(**args)

    for infile,id in files.items():
        unzip_path = f"{workspace}/{infile.split('/')[-1].split('.')[0]}"
        logger.info(f"Unzipping {infile} to {unzip_path}")

        try:
            csv_file = npi_loader.unzip(infile, unzip_path)
        except Exception as e:
            logger.info(e)
            logger.info(f"Error loading zip file, skipping file...")
            continue

        logger.info(f"Preprocessing {csv_file}")
        try:
            cleaned_file = npi_loader.preprocess(csv_file)
        except Exception as e:
            logger.info(e)
            logger.info(f"Error preprocessing file, skipping file...")
            continue
        
        try:
            if large_file:
                print(f"Loading {period} (large) file into database.  large_file: {large_file}")
                npi_loader.disable_checks()     # disable foreign key, unique checks, etc, for better performance
                npi_loader.load_large_file(table_name, cleaned_file)
                npi_loader.enable_checks()
            else:
                print(f"Loading {period} (small) file into database.  large_file: {large_file}")
                npi_loader.load_file(table_name, cleaned_file, batch_size)
        except Exception as e:
            logger.info(e)
            logger.info(f"Error loading data to DB, skipping file...")
            continue

        try:
            npi_loader.mark_imported(id, import_table_name)
        except Exception as e:
            logger.info(e)
            logger.info(f"Failed to update record in database.")

        # npi_loader.clean()
    npi_loader.close()

    logger.info(f"Data loaded to table: {table_name}")


start.add_command(npi)
npi.add_command(load)
npi.add_command(create)
npi.add_command(fetch)
npi.add_command(npi_unzip, name="unzip")
npi.add_command(npi_preprocess, name="preprocess")
npi.add_command(all)

if __name__ == '__main__':
    start()

