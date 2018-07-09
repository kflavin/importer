
import csv
import click
import _mysql
import mysql.connector as connector
import os
from loaders.npi import NpiLoader

@click.group()
def start():
    print("Starting...")

@click.command()
def hello():
    print("Say hello!")

@click.command()
@click.option('--infile', '-i', type=click.STRING, help="CSV file with NPI data")
@click.option('--batch-size', '-b', type=click.INT, help="Batch size")
@click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
def npi(infile, batch_size, step_load):
    print("Import NPI data")
    npi_loader = NpiLoader(user=os.environ['db_user'], host=os.environ['db_host'], password=os.environ['db_password'], database=os.environ['db_schema'], table_name="kyle_npi")
    # npi_loader.create_table()
    if step_load:
        print("Using Step Loader")
        npi_loader.step_load(infile, *step_load)
    else:
        npi_loader.load(open(infile, 'r'), batch_size=batch_size)

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

if __name__ == '__main__':
    start()

