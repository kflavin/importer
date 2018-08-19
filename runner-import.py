#!/usr/bin/env python3
import click
import os
from importer.loaders.npi import NpiLoader
from importer.loggers.cloudwatch import CloudWatchLogger

DEBUG = False

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--logging', '-l', type=click.STRING, help="[cloudwatch|system]")
def start(debug, logging):
    DEBUG = debug

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
@click.option('--url-prefix', '-u', required=True, type=click.STRING, help="URL directory that contains weekly or monthly files to download")
@click.option('--table-name', '-t', default="npi_import_log", type=click.STRING, help="Import log table")
@click.option('--period', '-p', required=True, type=click.STRING, help="[weekly|monthly]")
@click.option('--output_dir', '-o', default="/tmp/npi", type=click.STRING, help="Directory to store file on local filesystem")
@click.option('--limit', '-l', default=3, type=click.INT, help="Max # of files to download at a time.  Only weekly files are adjustable, monthly is set to 1.")
def fetch(url_prefix, table_name, period, output_dir, limit):
    """
    Download files from import log table
    """
    print(f"Download '{period}' files from {table_name}")
    npi_loader = NpiLoader()
    npi_loader.connect(user=os.environ['db_user'],
                        host=os.environ['db_host'], 
                        password=os.environ['db_password'], 
                        database=os.environ['db_schema'], 
                        dictionary=True, 
                        buffered=True)
    npi_loader.fetch(url_prefix, table_name, period, output_dir, limit)

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

    args = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema')
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
# @click.option('--unzip-path', '-u', default="/tmp/npi", type=click.STRING, help="Directory to extract zip")
@click.option('--url-prefix', '-u', required=True, type=click.STRING, help="URL directory that contains weekly or monthly files to download")
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="NPI table")
@click.option('--import-table-name', '-i', default="npi_import_log", type=click.STRING, help="NPI import table")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--workspace', '-w', default="/tmp/npi", type=click.STRING, help="Workspace directory")
@click.option('--limit', '-l', default=3, type=click.INT, help="Max # of files to download at a time.  Only weekly files are adjustable, monthly is set to 1.")
# @click.option('--output_dir', '-o', default="/tmp/npi", type=click.STRING, help="Local directory for downloaded files")
def all(url_prefix, batch_size, table_name, import_table_name, period, workspace, limit):
    """
    Perform all load steps.
    """
    # region = os.environ.get('aws_region')
    # logger = CloudWatchLogger("importer-npi", os.environ.get('instance_id'), region=region)
    # logger.info(f"Start: {period} file")

    workspace = workspace.rstrip("/")

    args = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema')
    }

    extra_args = {
        'dictionary': True,      # For referencing returned columns by name
        'buffered': True        # so we can get row counts without reading every row
    }

    # Download files listed in import log
    # logger.info(f"Download '{period}' files from {import_table_name}")
    print(f"Download '{period}' files from {import_table_name}")
    npi_downloader = NpiLoader()
    npi_downloader.connect(**{**args, **extra_args})
    files = npi_downloader.fetch(url_prefix, import_table_name, period, workspace, limit)
    npi_downloader.close()

    # Load the files
    # logger.info(f"Loading '{period}' files from {table_name}")
    print(f"Loading '{period}' files into {table_name}")
    npi_loader = NpiLoader()
    npi_loader.connect(**args)

    for infile,id in files.items():
        unzip_path = f"{workspace}/{infile.split('/')[-1].split('.')[0]}"
        # logger.info("Unzipping file to {unzip_path}")
        print(f"Unzipping {infile} to {unzip_path}")

        try:
            csv_file = npi_loader.unzip(infile, unzip_path)
        except Exception as e:
            print(e)
            print(f"Error loading zip file, skipping file...")
            continue

        # logger.info("Preprocessing file")
        print(f"Preprocessing {csv_file}")
        try:
            cleaned_file = npi_loader.preprocess(csv_file)
        except Exception as e:
            print(e)
            print(f"Error preprocessing file, skipping file...")
            continue
        
        try:
            if period.lower() == "weekly":
                # logger.info("Loading weekly NPI file into database")
                print(f"Loading {cleaned_file} (weekly) into database")
                npi_loader.load_weekly(table_name, cleaned_file, batch_size)
            else:
                # logger.info("Loading monthly NPI file into database")
                print(f"Loading {cleaned_file} (monthly) into database")
                npi_loader.disable_checks()     # disable foreign key, unique checks, etc, for better performance
                npi_loader.load_monthly(table_name, cleaned_file)
                npi_loader.enable_checks()
        except Exception as e:
            print(e)
            print(f"Error loading data to DB, skipping file...")
            continue

        try:
            npi_loader.mark_imported(id, import_table_name)
        except Exception as e:
            print(e)
            print(f"Failed to update record in database.")

        # npi_loader.clean()
    npi_loader.close()

    # logger.info(f"Data loaded to table: {table_name}")
    print(f"Data loaded to table: {table_name}")


start.add_command(npi)
npi.add_command(load)
npi.add_command(create)
npi.add_command(fetch)
npi.add_command(npi_unzip, name="unzip")
npi.add_command(npi_preprocess, name="preprocess")
npi.add_command(all)

if __name__ == '__main__':
    start()

