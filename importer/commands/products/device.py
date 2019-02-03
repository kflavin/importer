import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql import (INSERT_QUERY)
from importer.sql.products.device import CREATE_DEVICE_DDL, CREATE_DEVICEMASTER_DDL
from importer.commands.products.common import parseInt, parseIntOrNone

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def device(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load_gudid_devices(ctx, infile, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])

    loader.column_type_overrides = {
        # 'primarydi': (lambda x: int(float(x)) if x else None),
        'deviceid': (lambda x: parseIntOrNone(x)),
        'dunsnumber': (lambda x: parseIntOrNone(x)),
        'containsdinumber': (lambda x: parseIntOrNone(x)),
        'pkgquantity': (lambda x: parseIntOrNone(x)),
        'rx': (lambda x: True if x.lower() == "true" else False),
        'otc': (lambda x: True if x.lower() == "true" else False),
    }

    loader.connect(**ctx.obj['db_credentials'])
    loader.csv_loader(INSERT_QUERY, table_name, infile, ctx)

    print(f"Medical device data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.option('--complete/--no-complete', default=False, help="Complete file, or rx file.")
@click.pass_context
def load(ctx, infile, table_name, complete):
    loader = BaseLoader(warnings=ctx.obj['warnings'])

    if complete:
        loader.column_type_overrides = {
            # 'primarydi': (lambda x: int(float(x)) if x else None),
            'deviceid': (lambda x: parseIntOrNone(x)),
            'dunsnumber': (lambda x: parseIntOrNone(x)),
            'containsdinumber': (lambda x: parseIntOrNone(x)),
            'pkgquantity': (lambda x: parseIntOrNone(x)),
            'rx': (lambda x: True if x.lower() == "true" else False),
            'otc': (lambda x: True if x.lower() == "true" else False),
            'phoneextension': (lambda x: parseIntOrNone(x)),
            'eff_date': (lambda x: convert_date(x)),
            'end_eff_date': (lambda x: convert_date(x))
        }
    else:
        loader.column_type_overrides = {
            'rx_id': (lambda x: parseIntOrNone(x)),
            # 'deviceid': (lambda x: parseInt(x)),
            'containsdinumber': (lambda x: parseIntOrNone(x)),
            'dunsnumber': (lambda x: parseIntOrNone(x)),
            'eff_date': (lambda x: convert_date(x)),
            'end_eff_date': (lambda x: convert_date(x))
        }

    loader.connect(**ctx.obj['db_credentials'])
    loader.csv_loader(INSERT_QUERY, table_name, infile, ctx)

    print(f"Medical device data loaded to table: {table_name}")

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--complete/--no-complete', default=False, help="Complete file, or rx file.")
@click.pass_context
def create_table(ctx, table_name, complete):
    loader = ctx.obj['loader']
    logger.info(f"Creating table {table_name}...")

    if complete:
        q = CREATE_DEVICEMASTER_DDL.format(table_name=table_name)
    else:
        q = CREATE_DEVICE_DDL.format(table_name=table_name)

    loader._submit_single_q(q)

# def load(ctx, infile, table_name):
#     """
#     Med Device Loader
#     """
#     batch_size = ctx.obj['batch_size']
#     throttle_size = ctx.obj['throttle_size']
#     throttle_time = ctx.obj['throttle_time']
#     debug = ctx.obj['debug']

#     args = {
#         'user': os.environ.get('db_user'),
#         'password': os.environ.get('db_password'),
#         'host': os.environ.get('db_host'),
#         'database': os.environ.get('db_schema'),
#         'debug': debug
#     }

#     logger.debug("Loading: query={} table={} infile={} batch_size={} throttle_size={} throttle_time={} \n".format(
#         INSERT_AND_UPDATE_QUERY, table_name, infile, batch_size, throttle_size, throttle_time
#     ))

#     loader = BaseLoader()
#     loader.column_type_overrides = {
#         'rx': (lambda x: True if x.lower() == "true" else False),
#         'otc': (lambda x: True if x.lower() == "true" else False),
#         'phoneextension': (lambda x: float(int(x)) if x else None),
#         'containsdinumber': (lambda x: float(int(x)) if x else None),
#         'eff_date': (lambda x: convert_date(x)),
#         'end_eff_date': (lambda x: convert_date(x))
#     }
#     loader.warnings = True
#     logger.info(f"Loading {infile} into {table_name}")
#     loader.connect(**args)
#     loader.load_file(INSERT_AND_UPDATE_QUERY, table_name, infile, batch_size, throttle_size, throttle_time)

#     print(f"Data loaded to table: {table_name}")

# @click.command()
# @click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
# @click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
# def preprocess(infile, outfile):
#     """
#     Preprocess device files
#     """
#     loader = BaseLoader()
#     loader.preprocess(infile, outfile)
#     print(outfile)

device.add_command(load)
device.add_command(load_gudid_devices)
device.add_command(create_table)
# device.add_command(preprocess)
