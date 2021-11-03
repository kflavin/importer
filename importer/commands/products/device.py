import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql import (INSERT_QUERY)
from importer.sql.base import TRUNCATE_TABLE_DML
from importer.sql.products.device import CREATE_DEVICE_DDL, CREATE_DEVICEMASTER_DDL, CREATE_DEVICEMASTER_STAGE_DML
from importer.commands.products.common import parseInt, parseIntOrNone

logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def device(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader


@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load_gudid_devices(ctx, infile, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])

    loader.column_type_overrides = {
        # 'primarydi': (lambda x: int(float(x)) if x else None),
        'deviceid': (lambda x: parseIntOrNone(x)),
        'dunsnumber': (lambda x: parseIntOrNone(x)),
        'containsdinumber': (lambda x: parseIntOrNone(x)),
        'pkgquantity': (lambda x: parseIntOrNone(x)),
        'rx': (lambda x: True if x.lower() == "true" else False),
        'otc': (lambda x: True if x.lower() == "true" else False),
    }

    loader.connect(**ctx.obj['db_credentials'])
    loader.csv_loader(INSERT_QUERY, table_name, infile, ctx)

    print(f"Medical device data loaded to table: {table_name}")


@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load_device_staging_table(ctx, table_name):
    """
    Load the device master staging table.
    """
    loader = ctx.obj['loader']
    logger.info(f"Loading device master staging table: `{table_name}`...")

    refresh_devices_table_name = "refresh_gudid_devices"
    refresh_identifiers_table_name = "refresh_gudid_identifiers"
    refresh_contacts_table_name = "refresh_gudid_contacts"

    q1 = TRUNCATE_TABLE_DML.format(table_name=table_name)
    loader._submit_single_q(q1)

    q2 = CREATE_DEVICEMASTER_STAGE_DML.format(table_name=table_name,
                                              refresh_devices_table_name=refresh_devices_table_name,
                                              refresh_identifiers_table_name=refresh_identifiers_table_name,
                                              refresh_contacts_table_name=refresh_contacts_table_name)

    loader._submit_single_q(q2)
    logger.info("Finished.")


@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.option('--complete/--no-complete', default=False, help="Complete file, or rx file.")
@click.pass_context
def load(ctx, infile, table_name, complete):
    loader = BaseLoader(warnings=ctx.obj['warnings'])

    if complete:
        loader.column_type_overrides = {
            # 'primarydi': (lambda x: int(float(x)) if x else None),
            'deviceid': (lambda x: parseIntOrNone(x)),
            'dunsnumber': (lambda x: parseIntOrNone(x)),
            'containsdinumber': (lambda x: parseIntOrNone(x)),
            'pkgquantity': (lambda x: parseIntOrNone(x)),
            'rx': (lambda x: True if x.lower() == "true" else False),
            'otc': (lambda x: True if x.lower() == "true" else False),
            'phoneextension': (lambda x: parseIntOrNone(x)),
            'eff_date': (lambda x: convert_date(x)),
            'end_eff_date': (lambda x: convert_date(x))
        }
    else:
        loader.column_type_overrides = {
            'rx_id': (lambda x: parseIntOrNone(x)),
            # 'deviceid': (lambda x: parseInt(x)),
            'containsdinumber': (lambda x: parseIntOrNone(x)),
            'dunsnumber': (lambda x: parseIntOrNone(x)),
            'eff_date': (lambda x: convert_date(x)),
            'end_eff_date': (lambda x: convert_date(x))
        }

    loader.connect(**ctx.obj['db_credentials'])
    loader.csv_loader(INSERT_QUERY, table_name, infile, ctx)

    print(f"Medical device data loaded to table: {table_name}")


@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--complete/--no-complete', default=False, help="Complete file, or rx file.")
@click.pass_context
def create_table(ctx, table_name, complete):
    loader = ctx.obj['loader']
    logger.info(f"Creating table {table_name}...")

    if complete:
        q = CREATE_DEVICEMASTER_DDL.format(table_name=table_name)
    else:
        q = CREATE_DEVICE_DDL.format(table_name=table_name)

    loader._submit_single_q(q)


device.add_command(load)
device.add_command(load_gudid_devices)
device.add_command(create_table)
device.add_command(load_device_staging_table)
