import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.commands.products.common import data_loader
from importer.sql import (INSERT_QUERY)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def ndc(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load(ctx, infile, table_name):

    column_type_overrides = {
        'master_id': (lambda x: int(float(x)) if x else None),
        'eff_date': (lambda x: convert_date(x)),
        'end_eff_date': (lambda x: convert_date(x))
    }

    data_loader(BaseLoader, INSERT_QUERY, column_type_overrides, ctx, infile, table_name)
    print(f"NDC data loaded to table: {table_name}")

# @click.command()
# @click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with NDC Master data")
# @click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
# def preprocess(infile, outfile):
#     ndc_loader = BaseLoader()
#     ndc_loader.preprocess(infile, outfile)
#     print(outfile)

ndc.add_command(load)
# ndc.add_command(preprocess)