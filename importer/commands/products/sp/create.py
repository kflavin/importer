import click
import os
import logging

from importer.sql.products.sp.create_stage_tables import SP_CREATE_STAGINGTABLES
from importer.sql.products.sp.create_prod_tables import SP_CREATE_PRODTABLES
from importer.sql.products.sp.create_views import SP_CREATE_VIEWS
from importer.sql.products.sp.prep_product_keys import SP_PREP_PRODUCTKEYS
from importer.sql.products.sp.prep_device_master import SP_PREP_DEVICEMASTER
from importer.sql.products.sp.prep_drug_master import SP_PREP_DRUGMASTER
# from importer.sql.base import (CREATE_TABLE_LIKE_IFNE_DDL)
from importer.commands.products.sp import recreate_sp

logger = logging.getLogger(__name__)

@click.group()
@click.option('--user', '-u', required=True, type=click.STRING, help="")
@click.option('--prod_db', '-d', required=True, type=click.STRING, help="")
@click.pass_context
def create(ctx, user, prod_db):
    ctx.obj['user'] = user
    ctx.obj['prod_db'] = prod_db

@click.command()
@click.pass_context
def create_staging_tables(ctx):
    recreate_sp("sp_create_staging_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def create_prod_tables(ctx):
    recreate_sp("sp_create_prod_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def create_views(ctx):
    recreate_sp("sp_prep_device_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def prep_drug_prodkeys(ctx):
    recreate_sp("sp_prep_drug_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def prep_drug_master(ctx):
    recreate_sp("sp_prep_drug_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def prep_device_prodkeys(ctx):
    recreate_sp("sp_prep_device_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def prep_device_master(ctx):
    recreate_sp("sp_prep_device_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def prep_service_keys(ctx):
    recreate_sp("sp_prep_servicekeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])

@click.command()
@click.pass_context
def all(ctx):
    recreate_sp("sp_create_staging_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])
    recreate_sp("sp_create_prod_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])
    recreate_sp("sp_prep_drug_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])
    recreate_sp("sp_prep_drug_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])
    recreate_sp("sp_prep_device_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])
    recreate_sp("sp_prep_device_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])
    recreate_sp("sp_prep_servicekeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'], ctx.obj['prod_db'])


create.add_command(create_staging_tables)
create.add_command(create_prod_tables)
create.add_command(prep_drug_prodkeys)
create.add_command(prep_drug_master)
create.add_command(prep_device_prodkeys)
create.add_command(prep_device_master)
create.add_command(prep_service_keys)
create.add_command(all)