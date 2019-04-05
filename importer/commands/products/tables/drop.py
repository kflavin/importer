import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql.base import DROP_TABLE_IFE_DDL

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def drop(ctx):
    ctx.ensure_object(dict)

# @click.command()
# @click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
# @click.pass_context
# def indications(ctx, table_name):
#     loader = BaseLoader(warnings=ctx.obj['warnings'])
#     loader.connect(**ctx.obj['db_credentials'])

#     q = CREATE_INDICATIONS_DDL.format(table_name=table_name)
#     loader._query(q)

# @click.command()
# @click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
# @click.pass_context
# def ndc(ctx, table_name):
#     loader = BaseLoader(warnings=ctx.obj['warnings'])
#     loader.connect(**ctx.obj['db_credentials'])

#     q = CREATE_NDC_DDL.format(table_name=table_name)
#     loader._query(q)

# @click.command()
# @click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
# @click.pass_context
# def marketing(ctx, table_name):
#     loader = BaseLoader(warnings=ctx.obj['warnings'])
#     loader.connect(**ctx.obj['db_credentials'])

#     q = CREATE_MARKETING_DDL.format(table_name=table_name)
#     loader._query(q)

# @click.command()
# @click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
# @click.pass_context
# def orange(ctx, table_name):
#     loader = BaseLoader(warnings=ctx.obj['warnings'])
#     loader.connect(**ctx.obj['db_credentials'])

#     q = CREATE_ORANGE_DDL.format(table_name=table_name)
#     loader._query(q)

@click.command()
@click.pass_context
def all(ctx):
    """
    Create all product refresh tables using default names.
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    indications_table_name = "refresh_indications"
    ndc_table_name = "refresh_ndc_product"
    marketing_table_name = "refresh_marketing"
    orange_table_name = "refresh_orange"
    devicemaster_table_name = "refresh_devicemaster"
    guid_devices = "refresh_gudid_devices"
    guid_identifiers = "refresh_gudid_identifiers"
    guid_contacts = "refresh_gudid_contacts"

    q1 = DROP_TABLE_IFE_DDL.format(table_name=indications_table_name)
    q2 = DROP_TABLE_IFE_DDL.format(table_name=ndc_table_name)
    q3 = DROP_TABLE_IFE_DDL.format(table_name=marketing_table_name)
    q4 = DROP_TABLE_IFE_DDL.format(table_name=orange_table_name)
    q5 = DROP_TABLE_IFE_DDL.format(table_name=devicemaster_table_name)
    q6 = DROP_TABLE_IFE_DDL.format(table_name=guid_devices)
    q7 = DROP_TABLE_IFE_DDL.format(table_name=guid_identifiers)
    q8 = DROP_TABLE_IFE_DDL.format(table_name=guid_contacts)
    try:
        loader._query(q1)
        loader._query(q2)
        loader._query(q3)
        loader._query(q4)
        loader._query(q5)
    except Exception as e:
        print(e.args)
        print("Could not drop table.  Does the table exist?")
        return
        
    print(f"DROPPED refresh tables:\n {indications_table_name}\n {ndc_table_name}\n {marketing_table_name}\n {orange_table_name}")


# create.add_command(indications)
# create.add_command(ndc)
# create.add_command(marketing)
# create.add_command(orange)
drop.add_command(all)
