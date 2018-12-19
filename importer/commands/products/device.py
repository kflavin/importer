import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql import (INSERT_AND_UPDATE_QUERY)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def device(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load(ctx, infile, table_name):
    """
    Med Device Loader
    """
    batch_size = ctx.obj['batch_size']
    throttle_size = ctx.obj['throttle_size']
    throttle_time = ctx.obj['throttle_time']
    debug = ctx.obj['debug']

    args = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema'),
        'debug': debug
    }

    logger.debug("Loading: query={} table={} infile={} batch_size={} throttle_size={} throttle_time={} \n".format(
        INSERT_AND_UPDATE_QUERY, table_name, infile, batch_size, throttle_size, throttle_time
    ))

    loader = BaseLoader()
    loader.column_type_overrides = {
        'rx': (lambda x: True if x.lower() == "true" else False),
        'otc': (lambda x: True if x.lower() == "true" else False),
        'phoneextension': (lambda x: float(int(x)) if x else None),
        'containsdinumber': (lambda x: float(int(x)) if x else None),
        'eff_date': (lambda x: convert_date(x)),
        'end_eff_date': (lambda x: convert_date(x))
    }
    loader.warnings = True
    logger.info(f"Loading {infile} into {table_name}")
    loader.connect(**args)
    loader.load_file(INSERT_AND_UPDATE_QUERY, table_name, infile, batch_size, throttle_size, throttle_time)

    print(f"Data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
@click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
def preprocess(infile, outfile):
    """
    Preprocess device files
    """
    loader = BaseLoader()
    loader.preprocess(infile, outfile)
    print(outfile)

device.add_command(load)
device.add_command(preprocess)