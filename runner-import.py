#!/usr/bin/env python3
import csv
import click
import _mysql
import mysql.connector as connector
import os
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

    args = {
        'user': os.environ['db_user'],
        'password': os.environ['db_password'],
        'host': os.environ['db_host'],
        'database': os.environ['db_schema']
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
    if not outfile:
        outfile = infile[:infile.rindex(".")] + ".clean.csv"

    npi_loader = NpiLoader()
    npi_loader.preprocess(infile, outfile)
    # print(f"{outfile.split('/')[-1]}")
    print(outfile)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="File to unzip")
@click.option('--path', '-p', required=True, type=click.STRING, help="Directory to extract to")
def npi_unzip(infile, path):
    npi_loader = NpiLoader()
    csv_file = npi_loader.unzip(infile, path)
    print(csv_file)


start.add_command(npi)
npi.add_command(load)
npi.add_command(create)
npi.add_command(npi_unzip, name="unzip")
npi.add_command(npi_preprocess, name="preprocess")

if __name__ == '__main__':
    start()

