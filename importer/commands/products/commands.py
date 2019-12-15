import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql import (INSERT_QUERY)
from importer.sql.products.refresh.product import ALTER_PRODUCT_RELOAD_TABLE
from importer.sql.products.product import (CREATE_PRODUCT_MASTER_DDL, 
                            CREATE_PRODUCT_DDL, CREATE_PRODUCT_RELOAD_DDL)
from importer.commands.products.common import data_loader, parseIntOrNone

logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def product(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader


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
        'end_date': (lambda x: convert_date(x)),
        'created_date': (lambda x: convert_date(x)),
        'modified_date': (lambda x: convert_date(x)),
        'eff_date': (lambda x: convert_date(x)),
        'end_eff_date': (lambda x: convert_date(x))
    }
    
    data_loader(BaseLoader, INSERT_QUERY, column_type_overrides, ctx, infile, table_name)
    print(f"Complete product data loaded to table: {table_name}")

# Same as "products tables create"
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


@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.pass_context
def create_reload_table(ctx, table_name):
    loader = ctx.obj['loader']
    logger.info(f"Creating \"reload\" table {table_name}...")
    q = CREATE_PRODUCT_RELOAD_DDL.format(table_name=table_name)
    loader._submit_single_q(q)
    logger.info("Finished.")


@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.pass_context
def alter_reload_table(ctx, table_name):
    loader = ctx.obj['loader']
    logger.info(f"Creating \"reload\" table {table_name}...")
    q = ALTER_PRODUCT_RELOAD_TABLE.format(table_name=table_name)
    loader._submit_single_q(q)
    logger.info("Finished.")


product.add_command(load)
product.add_command(create_table)
product.add_command(create_reload_table)
product.add_command(alter_reload_table)
