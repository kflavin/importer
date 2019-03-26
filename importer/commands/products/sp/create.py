import click
import os
import logging

from importer.sql.products.sp.create_stage_tables import SP_CREATE_STAGINGTABLES
from importer.sql.products.sp.create_prod_tables import SP_CREATE_PRODTABLES
from importer.sql.products.sp.create_views import SP_CREATE_VIEWS
from importer.sql.products.sp.prep_product_keys import SP_PREP_PRODUCTKEYS
from importer.sql.products.sp.prep_device_master import SP_PREP_DEVICEMASTER
from importer.sql.products.sp.prep_drug_master import SP_PREP_DRUGMASTER
from importer.sql.products.sp.drop import DROP_SP
# from importer.sql.base import (CREATE_TABLE_LIKE_IFNE_DDL)

logger = logging.getLogger(__name__)

@click.group()
@click.option('--user', '-u', required=True, type=click.STRING, help="")
@click.pass_context
def create(ctx, user):
    ctx.obj['user'] = user

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
@click.pass_context
def updateall(ctx):
    loader = ctx.obj['loader']

    sp_names = {
        "create_prod_tables": "sp_create_prodtables",
        "create_stage_tables": "sp_create_stagetables",
        "create_view": "sp_create_views",
        "prep_productkeys": "sp_productkeys",
        "prep_devicemaster": "sp_devicemaster",
        "prep_drugmaster": "sp_drugmaster"
    }

    # Drop SP's
    for key,val in sp_names.items():
        loader._query(DROP_SP.format(database=ctx.obj['db_name'], procedure_name=val))

    # Recreate them
    q1 = SP_CREATE_PRODTABLES.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['create_prod_tables'])
    q2 = SP_CREATE_STAGINGTABLES.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['create_stage_tables'])
    q3 = SP_CREATE_VIEWS.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['create_view'])
    q4 = SP_PREP_DEVICEMASTER.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['prep_productkeys'])
    q5 = SP_PREP_DRUGMASTER.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['prep_devicemaster'])
    q6 = SP_PREP_PRODUCTKEYS.format(user=ctx.obj['user'], database=ctx.obj['db_name'], procedure_name=sp_names['prep_drugmaster'])

    loader._query(q1)
    loader._query(q2)
    loader._query(q3)
    loader._query(q4)
    loader._query(q5)
    loader._query(q6)
    print("SP's updated")

create.add_command(sp_prep_productkeys)
create.add_command(sp_create_stagingtables)
create.add_command(updateall)