#!/usr/bin/env python3
import csv
import click
import _mysql
import mysql.connector as connector
import os
import boto3
from importer.loaders.npi import NpiLoader

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
def all(infile, unzip_path, outfile, batch_size, table_name, period):
    """
    Perform all steps
    """

    npi_loader = NpiLoader()
    print("Unzipping file")
    csv_file = npi_loader.unzip(infile, unzip_path)
    print("Preprocessing file")
    cleaned_file = npi_loader.preprocess(csv_file, outfile)

    client = boto3.client('ssm', region_name=os.environ['aws_region'])
    args = {
        'user': os.environ.get('db_user', client.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value']),
        'password': os.environ.get('db_password', client.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value']),
        'host': os.environ.get('db_host', client.get_parameter(Name='db_host')['Parameter']['Value']),
        'database': os.environ.get('db_schema', client.get_parameter(Name='db_schema', WithDecryption=True)['Parameter']['Value'])
    }

    if period.lower() == "weekly":
        print("Loading weekly NPI file")
        npi_loader.connect(**args)
        npi_loader.load_weekly(table_name, cleaned_file, batch_size)
    else:
        print("Loading monthly NPI file")
        npi_loader.connect(**args, clientFlags=True)
        npi_loader.load_monthly(table_name, cleaned_file)

    npi_loader.mark_imported(bucket_name, bucket_key)
    print(f"Data loaded to table: {table_name}")

@click.command()
def disable():
    print("Disable stuff")
    npi_loader = NpiLoader()
    client = boto3.client('ssm', region_name=os.environ['aws_region'])
    args = {
        'user': os.environ.get('db_user', client.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value']),
        'password': os.environ.get('db_password', client.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value']),
        'host': os.environ.get('db_host', client.get_parameter(Name='db_host')['Parameter']['Value']),
        'database': os.environ.get('db_schema', client.get_parameter(Name='db_schema', WithDecryption=True)['Parameter']['Value'])
    }
    npi_loader.connect(**args)
    npi_loader.disable_checks()

start.add_command(npi)
npi.add_command(load)
npi.add_command(create)
npi.add_command(npi_unzip, name="unzip")
npi.add_command(npi_preprocess, name="preprocess")
npi.add_command(all)
npi.add_command(disable)

if __name__ == '__main__':
    start()

