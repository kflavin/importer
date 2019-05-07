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

sp_names = {
        "create_prod_tables": "sp_create_prodtables",
        "create_staging_tables": "sp_create_staging_tables",
        "create_view": "sp_create_views",
        "prep_drug_prodkeys": "sp_productkeys",
        "prep_devicemaster": "sp_devicemaster",
        "prep_drug_master": "sp_drugmaster"
    }

@click.group()
@click.option('--user', '-u', required=True, type=click.STRING, help="")
@click.pass_context
def create(ctx, user):
    ctx.obj['user'] = user

@click.command()
@click.pass_context
def create_staging_tables(ctx):
    recreate_sp("create_staging_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])

@click.command()
@click.pass_context
def create_prod_tables(ctx):
    recreate_sp("create_prod_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])

@click.command()
@click.pass_context
def create_views(ctx):
    recreate_sp("prep_device_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])

@click.command()
@click.pass_context
def prep_drug_prodkeys(ctx):
    recreate_sp("prep_drug_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])

@click.command()
@click.pass_context
def prep_drug_master(ctx):
    recreate_sp("prep_drug_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])

@click.command()
@click.pass_context
def prep_device_prodkeys(ctx):
    recreate_sp("prep_device_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])

@click.command()
@click.pass_context
def prep_device_master(ctx):
    recreate_sp("prep_device_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])


@click.command()
@click.pass_context
def all(ctx):
    recreate_sp("create_staging_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])
    recreate_sp("create_prod_tables", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])
    recreate_sp("prep_drug_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])
    recreate_sp("prep_drug_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])
    recreate_sp("prep_device_prodkeys", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])
    recreate_sp("prep_device_master", ctx.obj['loader'], ctx.obj['user'], ctx.obj['db_name'])
    

#
# Deprecated functions below
#


@click.command()
@click.option('--procedure-name', '-p', required=True, type=click.STRING, help="")
@click.pass_context
def sp_prep_productkeys(ctx, procedure_name):
    loader = ctx.obj['loader']
    q = SP_PREP_PRODUCTKEYS.format(database=ctx.obj['db_name'], user=ctx.obj['user'], procedure_name=procedure_name)
    loader._query(q)
    print("Created SP")


@click.command()
@click.option('--procedure-name', '-p', required=True, type=click.STRING, help="")
@click.pass_context
def sp_create_stagingtables(ctx, procedure_name):
    loader = ctx.obj['loader']
    q = SP_CREATE_STAGINGTABLES.format(database=ctx.obj['db_name'], user=ctx.obj['user'], procedure_name=procedure_name)
    loader._query(q)
    print("Created SP")

@click.command()
@click.option('--procedure-name', '-p', required=False, default=None, type=click.STRING, help="")
@click.pass_context
def sp_create_views(ctx, procedure_name):
    loader = ctx.obj['loader']
    pn = procedure_name if procedure_name else "sp_create_views"

    loader._query(DROP_SP.format(database=ctx.obj['db_name'], procedure_name=pn))
    q = SP_CREATE_VIEWS.format(database=ctx.obj['db_name'], user=ctx.obj['user'], procedure_name=pn)
    loader._query(q)
    print("Created view SP")

@click.command()
@click.pass_context
def updateall(ctx):
    loader = ctx.obj['loader']

    # Drop SP's
    for key,val in sp_names.items():
        loader._query(DROP_SP.format(database=ctx.obj['db_name'], procedure_name=val))

    # Recreate them
    q1 = SP_CREATE_PRODTABLES.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['create_prod_tables'])
    q2 = SP_CREATE_STAGINGTABLES.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['create_stage_tables'])
    q3 = SP_CREATE_VIEWS.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['create_view'])
    q4 = SP_PREP_DEVICEMASTER.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['prep_devicemaster'])
    q5 = SP_PREP_DRUGMASTER.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['prep_drugmaster'])
    q6 = SP_PREP_PRODUCTKEYS.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['prep_productkeys'])

    loader._query(q1)
    loader._query(q2)
    loader._query(q3)
    loader._query(q4)
    loader._query(q5)
    loader._query(q6)
    print("SP's updated")

create.add_command(sp_prep_productkeys)
create.add_command(sp_create_stagingtables)
create.add_command(sp_create_views)
create.add_command(updateall)

# New functions
create.add_command(create_staging_tables)
create.add_command(create_prod_tables)
create.add_command(prep_drug_prodkeys)
create.add_command(prep_drug_master)
create.add_command(prep_device_prodkeys)
create.add_command(prep_device_master)
create.add_command(all)