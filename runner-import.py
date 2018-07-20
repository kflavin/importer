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

@click.command()
def hello():
    print("Say hello!")

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
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size")
@click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="Table name to load.")
def load(infile, batch_size, step_load, table_name):
    """
    The NPI importer
    """
    print("Import NPI data...")
    npi_loader = NpiLoader()
    npi_loader.connect(user=os.environ['db_user'], host=os.environ['db_host'], password=os.environ['db_password'], database=os.environ['db_schema'])

    if step_load:
        print("Using Step Loader")
        npi_loader.step_load(table_name, infile, *step_load)
    else:
        npi_loader.load(table_name, open(infile, 'r'), batch_size=batch_size)
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
    print(f"{outfile.split('/')[-1]}")

@click.command()
@click.argument('infile', type=click.File('r'))
def readCsv(infile):
    print("Import CSV file")
    # content = infile.read().encode('utf-8')

    cnx = connector.connect(user=os.environ['db_user'], host=os.environ['db_host'], database=os.environ['db_schema'])
    cursor = cnx.cursor()

    insert_query = ("INSERT INTO kyle_doctor2 "
                 "(First_Name, Last_Name, NPI) "
                 "VALUES (%(first)s, %(last)s, %(npi)s) "
                 "ON DUPLICATE KEY UPDATE "
                 "First_Name = VALUES(First_Name), "
                 "Last_Name = VALUES(Last_Name), "
                 "NPI = VALUES(NPI)"
                 )

    reader = csv.DictReader(infile)
    all_data = []
    for row in reader:
        first = row['Provider First Name']
        last = row['Provider Last Name (Legal Name)']
        npi = row['NPI']

        data = {
            "npi": row['NPI'],
            "first": row['Provider First Name'],
            "last": row['Provider Last Name (Legal Name)']
        }

        all_data.append(data)

    # print(all_data)
    print("Execute query")
    cursor.executemany(insert_query, all_data)
    cnx.commit()

    print("All done")

start.add_command(readCsv, name="csv")
start.add_command(npi)
start.add_command(hello)
npi.add_command(load)
npi.add_command(create)
npi.add_command(npi_preprocess, name="preprocess")

if __name__ == '__main__':
    start()

