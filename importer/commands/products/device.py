import click
import os
import logging

from importer.loaders.products.device import DeviceLoader
from importer.sql.base import (INSERT_QUERY)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def device(ctx):
    ctx.ensure_object(dict)
    # ctx.obj['batch_size'] = batch_size
    # ctx.obj['throttle_size'] = throttle_size
    # ctx.obj['throttle_time'] = throttle_time

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load(ctx, infile, table_name):
    """
    NPI importer
    """
    batch_size = ctx.obj['batch_size']
    throttle_size = ctx.obj['throttle_size']
    throttle_time = ctx.obj['throttle_time']

    args = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema')
    }

    loader = DeviceLoader()

    # print(f"Loading {period} (small) file into database.  large_file: {large_file}")
    logger.info(f"Loading {infile} into {table_name}")
    loader.connect(**args)
    loader.load_file(INSERT_QUERY, table_name, infile, batch_size, throttle_size, throttle_time)

    print(f"Data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
@click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
def preprocess(infile, outfile):
    """
    Preprocess NPI csv file to do things like remove extraneous columns
    """
    product_loader = DeviceLoader()
    product_loader.preprocess(infile, outfile)
    print(outfile)

device.add_command(load)
device.add_command(preprocess)
