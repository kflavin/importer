
import click
import os
import logging

from importer.loaders.products.base import ProductBaseLoader
from importer.sql import DELETE_Q
from importer.sql.products.delta import (DELTA_NDC_TO_PRODUCT_MASTER_Q as DELTA_Q, 
                                        RETRIEVE_NDC_Q as RETRIEVE_LEFT_Q,
                                        RETRIEVE_PRODUCT_MASTER_Q as RETRIEVE_RIGHT_Q,
                                        INSERT_PRODUCT_MASTER_Q as INSERT_Q,
                                        ARCHIVE_PRODUCT_MASTER_Q as ARCHIVE_Q)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def delta(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def ndc_to_product_master(ctx, left_table_name, right_table_name, right_table_name_archive):
    loader = ProductBaseLoader(warnings=ctx.obj['warnings'], 
                        batch_size=ctx.obj['batch_size'], 
                        throttle_size=ctx.obj['throttle_size'], 
                        throttle_time=ctx.obj['throttle_time'])
    loader.connect(**ctx.obj['db_credentials'])
    join_columns = ["master_id"]
    compare_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    insert_archive_columns = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    insert_new_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    # Upper case the second and third columns (proprietary and non-proprietary name)
    # left_table_transforms = {
    #     2: (lambda x: x.upper() if x else None),
    #     3: (lambda x: x.upper() if x else None)
    # }
    left_table_transforms = {
        "proprietaryname": (lambda x: x.upper() if x else None),
        "nonproprietaryname": (lambda x: x.upper() if x else None)
    }
    right_table_transforms = {}
    if not right_table_name_archive:
        right_table_name_archive = f"{right_table_name}_archive"
    loader.delta_table(DELTA_Q, 
                        RETRIEVE_LEFT_Q,
                        RETRIEVE_RIGHT_Q,
                        DELETE_Q,
                        ARCHIVE_Q,
                        INSERT_Q,
                        join_columns,
                        compare_columns,
                        insert_archive_columns,
                        insert_new_columns,
                        left_table_transforms,
                        right_table_transforms,
                        left_table_name, 
                        right_table_name,
                        right_table_name_archive)

delta.add_command(ndc_to_product_master)