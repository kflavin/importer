import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql.products.refresh.tables import (CREATE_INDICATIONS_DDL, CREATE_NDC_DDL,
    CREATE_MARKETING_DDL, CREATE_ORANGE_DDL)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def create(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def indications(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_INDICATIONS_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def ndc(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_NDC_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def marketing(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_MARKETING_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def orange(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_ORANGE_DDL.format(table_name=table_name)
    loader._query(q)

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
    marketing_table_name = "refresh_marketing_codes"
    orange_table_name = "refresh_orange"

    q1 = CREATE_INDICATIONS_DDL.format(table_name=indications_table_name)
    q2 = CREATE_NDC_DDL.format(table_name=ndc_table_name)
    q3 = CREATE_MARKETING_DDL.format(table_name=marketing_table_name)
    q4 = CREATE_ORANGE_DDL.format(table_name=orange_table_name)
    loader._query(q1)
    loader._query(q2)
    loader._query(q3)
    loader._query(q4)

    print(f"CREATED refresh tables:\n {indications_table_name}\n {ndc_table_name}\n {marketing_table_name}\n {orange_table_name}")

create.add_command(indications)
create.add_command(ndc)
create.add_command(marketing)
create.add_command(orange)
create.add_command(all)
