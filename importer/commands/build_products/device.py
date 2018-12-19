import click
import os
import logging
import html

from importer.loaders.build_products.device import MedDeviceCompleteLoader, BaseLoader, convert_date
from importer.sql import (INSERT_QUERY)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def device(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--indir', '-i', required=True, type=click.STRING, help="Directory with XML files.")
# @click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load(ctx, indir, table_name):
    """
    Med Device Loader
    """
    batch_size = ctx.obj['batch_size']
    throttle_size = ctx.obj['throttle_size']
    throttle_time = ctx.obj['throttle_time']
    debug = ctx.obj['debug']
    warnings = ctx.obj['warnings']
    args = ctx.obj['db_credentials']

    logger.info("Loading: query={} table={} infile={} batch_size={} throttle_size={} throttle_time={} \n".format(
        INSERT_QUERY, table_name, indir, batch_size, throttle_size, throttle_time
    ))

    loader = MedDeviceCompleteLoader(warnings=warnings)
    loader.column_type_overrides = {
        'rx': (lambda x: True if x.lower() == "true" else False),
        'otc': (lambda x: True if x.lower() == "true" else False)
        # 'phoneextension': (lambda x: float(int(x)) if x else None),
        # 'containsdinumber': (lambda x: float(int(x)) if x else None),
        # 'eff_date': (lambda x: convert_date(x)),
        # 'end_eff_date': (lambda x: convert_date(x))
    }
    loader.all_columns_xform = [html.unescape]
    logger.info(f"Loading {indir} into {table_name}")
    loader.connect(**args)
    loader.load_xml_files(INSERT_QUERY, indir, table_name, batch_size, throttle_size, throttle_time)

    print(f"Data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
@click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
def preprocess(infile, outfile):
    """
    Preprocess device files
    """
    loader = BaseLoader()
    loader.preprocess(infile, outfile)
    print(outfile)

@click.command()
@click.option('--stage-table-name', '-s', required=True, type=click.STRING, help="")
@click.option('--prod-table-name', '-p', required=True, type=click.STRING, help="")
@click.pass_context
def delta(ctx, stage_table_name, prod_table_name):
    """
    Preprocess device files
    """
    loader = MedDeviceCompleteLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    arr = loader.delta_stage_to_prod(stage_table_name, prod_table_name, ctx.obj['batch_size'], ctx.obj['throttle_size'], ctx.obj['throttle_time'])
    # print(arr[0])
    # print(arr[1])

device.add_command(load)
device.add_command(preprocess)
device.add_command(delta)
