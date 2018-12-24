import click
import logging

from importer.loaders.base import BaseLoader
from importer.sql.products.merge.product_table import (PRODUCT_TABLE_MERGE)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def merge(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--old-table-name', '-o', required=True, type=click.STRING, help="")
@click.option('--new-table-name', '-n', required=True, type=click.STRING, help="")
@click.option('--master-table-name', '-m', required=True, type=click.STRING, help="")
@click.pass_context
def product_table(ctx, old_table_name, new_table_name, master_table_name):

    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    format_args = {
        "old_table_name": old_table_name,
        "new_table_name": new_table_name,
        "product_master": master_table_name
    }
    loader.execute_queries(PRODUCT_TABLE_MERGE, **format_args)

merge.add_command(product_table)

