import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql import (INSERT_QUERY)
from importer.sql.products.product import CREATE_PRODUCT_MASTER_DDL, CREATE_PRODUCT_DDL
from importer.commands.products.common import data_loader, parseIntOrNone

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def product(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader

# @click.command()
# @click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
# @click.pass_context
# def load(ctx, infile, table_name):
#     """
#     Product importer
#     """
#     batch_size = ctx.obj['batch_size']
#     throttle_size = ctx.obj['throttle_size']
#     throttle_time = ctx.obj['throttle_time']

#     args = {
#         'user': os.environ.get('db_user'),
#         'password': os.environ.get('db_password'),
#         'host': os.environ.get('db_host'),
#         'database': os.environ.get('db_schema')
#     }

#     loader = BaseLoader()
#     loader.column_type_overrides = {
#         'eff_date': (lambda x: convert_date(x)),
#         'end_eff_date': (lambda x: convert_date(x))
#     }
#     loader.warnings = True

#     # print(f"Loading {period} (small) file into database.  large_file: {large_file}")
#     logger.info(f"Loading {infile} into {table_name}")
#     loader.connect(**args)
#     loader.load_file(INSERT_AND_UPDATE_QUERY, table_name, infile, batch_size, throttle_size, throttle_time)

#     print(f"Data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.option('--complete/--no-complete', default=False, help="Complete file, or rx file.")
@click.pass_context
def load(ctx, infile, table_name, complete):
    loader = ctx.obj['loader']

    if complete:
        column_type_overrides = {
        'master_id': (lambda x: parseIntOrNone(x)),
        'eff_date': (lambda x: convert_date(x)),
        'end_eff_date': (lambda x: convert_date(x))
    }
    else:
        column_type_overrides = {
        'id': (lambda x: parseIntOrNone(x)),
        'master_id': (lambda x: int(float(x)) if x else None),
        'eff_date': (lambda x: convert_date(x)),
        'end_eff_date': (lambda x: convert_date(x))
    }
    
    data_loader(BaseLoader, INSERT_QUERY, column_type_overrides, ctx, infile, table_name)
    print(f"Complete product data loaded to table: {table_name}")

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--complete/--no-complete', default=False, help="Complete file, or rx file.")
@click.pass_context
def create_table(ctx, table_name, complete):
    loader = ctx.obj['loader']
    logger.info(f"Creating table {table_name}...")

    if complete:
        q = CREATE_PRODUCT_MASTER_DDL.format(table_name=table_name)
    else:
        q = CREATE_PRODUCT_DDL.format(table_name=table_name)

    loader._submit_single_q(q)
    logger.info("Finished.")

# @click.command()
# @click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
# @click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
# @click.option('--encoding', default="utf-8", type=click.STRING, help="Process Excel file (CSV by default)")
# @click.option('--excel', type=click.BOOL, help="Process Excel file (CSV by default)")
# def preprocess(infile, outfile, encoding, excel):
#     """
#     Preprocess Product files
#     """
#     product_loader = BaseLoader()
#     product_loader.preprocess(infile, outfile, encoding, excel)
#     print(outfile)

product.add_command(load)
product.add_command(create_table)
# product.add_command(preprocess)
