import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.loaders.gudid.base import GudidLoader
from importer.sql import (INSERT_AND_UPDATE_QUERY, SELECT_Q)
from importer.commands.products.common import data_loader, parseIntOrNone

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def gudid(ctx):
    ctx.ensure_object(dict)
    ctx.obj['db_credentials']['dictionary'] = True
    stage_loader = GudidLoader(warnings=ctx.obj['warnings'])
    stage_loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['stage_loader'] = stage_loader

    prod_db_credentials = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': "prod",
        'debug': ctx.obj['db_credentials']['debug']
    }

    logger.info(ctx.obj['db_credentials'])
    logger.info(prod_db_credentials)

    prod_loader = GudidLoader(warnings=ctx.obj['warnings'])
    prod_loader.connect(**prod_db_credentials)
    ctx.obj['prod_loader'] = prod_loader




@click.command()
# @click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--stage-table-name', '-s', required=True, type=click.STRING, help="Stage table name.")
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="Target table name.")
@click.pass_context
def load_devices(ctx, stage_table_name, target_table_name):
    prod_loader = ctx.obj['prod_loader']
    stage_loader = ctx.obj['stage_loader']

    logger.info("Select data from staging table")
    select_q = SELECT_Q.format(table_name="stage_fda_device")
    cursor = stage_loader._query(select_q, returnCursor=True)
    logger.info("Data selected")
    columns = cursor.column_names
    rows = list(cursor)
    logger.info("Pull data into list")
    logger.info("Start row loader")
    prod_loader.row_loader(INSERT_AND_UPDATE_QUERY, columns, rows, target_table_name, batch_size=ctx.obj['batch_size'], throttle_size=ctx.obj['throttle_size'], throttle_time=ctx.obj['throttle_time'])
    logger.info("Finish row loader")

    # column_type_overrides = {
    #     'id': (lambda x: parseIntOrNone(x)),
    #     'master_id': (lambda x: int(float(x)) if x else None),
    #     'end_date': (lambda x: convert_date(x)),
    #     'created_date': (lambda x: convert_date(x)),
    #     'modified_date': (lambda x: convert_date(x)),
    #     'eff_date': (lambda x: convert_date(x)),
    #     'end_eff_date': (lambda x: convert_date(x))
    # }
    # column_type_overrides = {}
    
    # data_loader(BaseLoader, INSERT_QUERY, column_type_overrides, ctx, infile, table_name)

    # loader.load_devices(INSERT_AND_UPDATE_QUERY, stage_table_name, target_table_name, batch_size=ctx.obj['batch_size'], throttle_size=ctx.obj['throttle_size'], throttle_time=ctx.obj['throttle_time'])
    print(f"Load gudid device data from {stage_table_name} to: {target_table_name}")

@click.command()
@click.option('--stage-table-name', '-s', required=True, type=click.STRING, help="Stage table name.")
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="Target table name.")
@click.pass_context
def load_identifiers(ctx, stage_table_name, target_table_name):
    prod_loader = ctx.obj['prod_loader']
    stage_loader = ctx.obj['stage_loader']
    gudid_loader(ctx, stage_loader, prod_loader, stage_table_name, target_table_name)
    print(f"Loaded gudid identifier data from {stage_table_name} to: {target_table_name}")

@click.command()
@click.option('--stage-table-name', '-s', required=True, type=click.STRING, help="Stage table name.")
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="Target table name.")
@click.pass_context
def load_contacts(ctx, stage_table_name, target_table_name):
    prod_loader = ctx.obj['prod_loader']
    stage_loader = ctx.obj['stage_loader']
    gudid_loader(ctx, stage_loader, prod_loader, stage_table_name, target_table_name)
    print(f"Loaded gudid contacts data from {stage_table_name} to: {target_table_name}")

@click.command()
@click.option('--stage-table-name', '-s', required=True, type=click.STRING, help="Stage table name.")
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="Target table name.")
@click.pass_context
def all(ctx, stage_table_name, target_table_name):
    prod_loader = ctx.obj['prod_loader']
    stage_loader = ctx.obj['stage_loader']
    gudid_loader(ctx, stage_loader, prod_loader, stage_table_name, target_table_name)
    gudid_loader(ctx, stage_loader, prod_loader, stage_table_name, target_table_name)
    gudid_loader(ctx, stage_loader, prod_loader, stage_table_name, target_table_name)
    print(f"Loaded gudid identifier data from {stage_table_name} to: {target_table_name}")


def gudid_loader(ctx, stage_loader, prod_loader, stage_table_name, prod_table_name):
    logger.info("Select data from staging table")
    select_q = SELECT_Q.format(table_name=stage_table_name)
    cursor = stage_loader._query(select_q, returnCursor=True)
    logger.info("Data selected")
    columns = cursor.column_names
    rows = list(cursor)
    logger.info("Pull data into list")
    logger.info("Start row loader")
    prod_loader.row_loader(INSERT_AND_UPDATE_QUERY, columns, rows, prod_table_name, batch_size=ctx.obj['batch_size'], throttle_size=ctx.obj['throttle_size'], throttle_time=ctx.obj['throttle_time'])
    logger.info("Finish row loader")

gudid.add_command(load_devices)
gudid.add_command(load_identifiers)
gudid.add_command(load_contacts)

