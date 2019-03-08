import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.commands.products.common import data_loader, parseIntOrNone
from importer.sql.products.ndc import CREATE_NDCMASTER_DDL
from importer.sql import (INSERT_QUERY)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def ndc(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load(ctx, infile, table_name):

    column_type_overrides = {
        'master_id': (lambda x: parseIntOrNone(x)),
        'eff_date': (lambda x: convert_date(x)),
        'end_eff_date': (lambda x: convert_date(x))
    }

    data_loader(BaseLoader, INSERT_QUERY, column_type_overrides, ctx, infile, table_name)
    print(f"NDC data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with NDC Master data")
@click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
@click.pass_context
def preprocess(ctx, infile, outfile):
    ndc_loader = BaseLoader()

    # xforms performs transformations on the column names.  
    # If the value is callable, call it on the column value (key)
    # Otherwise, just replace with the given value.
    xforms = {
        "Type": "te_type",
        "drug_id": "ind_drug_id",
        "status": "ind_status",
        "phase": "ind_phase",
    }
    ndc_loader.preprocess(infile, outfile, column_xforms=xforms)
    print(outfile)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Excel file with NDC Master data")
@click.pass_context
def create_table(ctx, table_name):
    loader = ctx.obj['loader']

    logger.info(f"Creating table {table_name}...")
    q = CREATE_NDCMASTER_DDL.format(table_name=table_name)
    loader._submit_single_q(q)
    logger.info("Finished.")

ndc.add_command(load)
ndc.add_command(preprocess)
ndc.add_command(create_table)