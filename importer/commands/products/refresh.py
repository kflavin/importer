import click
import os
import logging
import html

from importer.loaders.base import BaseLoader, convert_date
from importer.loaders.products.refresh import RefreshLoader
from importer.sql import (INSERT_QUERY)
from importer.sql.products.refresh.ndc import (REFRESH_NDC_TABLE_LOAD_INDICATIONS, REFRESH_NDC_TABLE_LOAD_ORANGE)
from importer.sql.products.refresh.device import POPULATE_DEVICE_REFRESH_TABLE
from importer.sql.base import (DROP_TABLE_DDL, DROP_TABLE_IFE_DDL, RENAME_TABLE_DDL, 
                CREATE_TABLE_LIKE_DDL, CREATE_TABLE_LIKE_IFNE_DDL)

logger = logging.getLogger(__name__)

# def parseInt(value):
#     try:
#         value = int(value)
#     except ValueError as e:
#         pass
#     return value

@click.group()
@click.pass_context
def refresh(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader

@click.command()
# @click.option('--target-table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--source-table-name', '-s', required=True, type=click.STRING, help="")
@click.option('--indications-table-name', '-i', required=True, type=click.STRING, help="")
@click.option('--ndc-product-table-name', '-n', required=True, type=click.STRING, help="")
@click.option('--orange-table-name', '-o', required=True, type=click.STRING, help="")
@click.pass_context
def ndc(ctx, 
    # target_table_name,
             source_table_name,
             indications_table_name,
             ndc_product_table_name,
             orange_table_name):
    """
    Load the full NDC refresh table.
    """
    loader = ctx.obj['loader']
    target_table_name = f"{source_table_name}_stage"
    # target_table_name2 = target_table_name + "2"
    # source_backup_table_name = source_table_name + "_backup"

    # target_table_name2 is the final table
    # logger.info(f"Creating tables {target_table_name} and {target_table_name2}...")
    q1 = CREATE_TABLE_LIKE_DDL.format(target_table_name=target_table_name, source_table_name=source_table_name)
    # q2 = CREATE_TABLE_LIKE_DDL.format(new_table_name=target_table_name2, old_table_name=source_table_name)
    logger.debug(q1)
    # logger.debug(q2)
    loader._submit_single_q(q1)
    # loader._submit_single_q(q2)
    logger.info("Finished.")

    logger.info("Loading indication data...")
    q3 = REFRESH_NDC_TABLE_LOAD_INDICATIONS.format(target_table_name   = target_table_name,
                                                   source_table_name   = source_table_name,
                                                   indications_table_name  = indications_table_name,
                                                   ndc_product_table_name  = ndc_product_table_name)
    logger.debug(q3)
    loader._submit_single_q(q3)
    logger.info("Finished.")

    logger.info("Loading TE Codes...")
    # q4 = REFRESH_NDC_TABLE_LOAD_ORANGE.format(target_table_name2=target_table_name2,
    #                                      target_table_name=target_table_name,
    #                                      orange_table_name=orange_table_name)
    q4 = REFRESH_NDC_TABLE_LOAD_ORANGE.format(
                                         target_table_name=target_table_name,
                                         orange_table_name=orange_table_name)
    logger.debug(q4)
    loader._submit_single_q(q4)

    # Remove temporary tables, and set target as the new table
    # DROP_TABLE_DDL.format(table_name=target_table_name)
    # loader._submit_single_q(DROP_TABLE_IFE_DDL.format(table_name=source_backup_table_name))
    # loader._submit_single_q(RENAME_TABLE_DDL.format(old_table_name=source_table_name, new_table_name=source_backup_table_name))
    # RENAME_TABLE_DDL.format(old_table_name=target_table_name2, new_table_name=source_table_name)
    # loader._submit_single_q(RENAME_TABLE_DDL.format(old_table_name=target_table_name, new_table_name=source_table_name))

    logger.info("Finished.")

@click.command()
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--source-table-name', '-s', required=True, type=click.STRING, help="")
@click.option('--indications-table-name', '-i', required=True, type=click.STRING, help="")
@click.option('--ndc-product-table-name', '-n', required=True, type=click.STRING, help="")
@click.pass_context
def ndc_load_indications(ctx, target_table_name, source_table_name, indications_table_name, ndc_product_table_name):
    """
    Load indications into the refresh NDC table.
    """
    loader = ctx.obj['loader']
    logger.info("Loading indication data...")
    q = REFRESH_NDC_TABLE_LOAD_INDICATIONS.format(target_table_name   = target_table_name,
                                                   source_table_name   = source_table_name,
                                                   indications_table_name  = indications_table_name,
                                                   ndc_product_table_name  = ndc_product_table_name)
    logger.debug(q)
    loader._submit_single_q(q)
    logger.info("Finished.")

@click.command()
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--orange-table-name', '-o', required=True, type=click.STRING, help="")
@click.pass_context
def ndc_load_orange(ctx, target_table_name, orange_table_name):
    """
    Load orange data into the refresh NDC table.
    """
    loader = ctx.obj['loader']
    logger.info("Loading TE Codes...")
    target_table_name2 = target_table_name + "2"
    q4 = REFRESH_NDC_TABLE_LOAD_ORANGE.format(target_table_name2=target_table_name2,
                                         target_table_name=target_table_name,
                                         orange_table_name=orange_table_name)
    logger.debug(q4)
    loader._submit_single_q(q4)
    logger.info("Finished.")

@click.command()
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--source-table-name', '-s', required=True, type=click.STRING, help="")
@click.pass_context
def ndc_create_tables(ctx, target_table_name, source_table_name):
    """
    Create NDC refresh tables.
    """
    loader = ctx.obj['loader']
    target_table_name2 = target_table_name + "2"
    stage_table_name = source_table_name + "_stage"

    logger.info(f"Creating tables {target_table_name} and {target_table_name2}...")
    q1 = CREATE_TABLE_LIKE_DDL.format(target_table_name=target_table_name, source_table_name=source_table_name)
    q2 = CREATE_TABLE_LIKE_DDL.format(target_table_name=target_table_name2, source_table_name=source_table_name)
    logger.debug(q1)
    logger.debug(q2)
    loader._submit_single_q(q1)
    loader._submit_single_q(q2)
    logger.info("Finished.")

@click.command()
@click.option('--indir', '-i', required=True, type=click.STRING, help="Directory with XML files.")
# @click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load_medical_device(ctx, indir, table_name):
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

    loader = RefreshLoader(warnings=warnings)
    loader.column_type_overrides = {
        'rx': (lambda x: True if x.lower() == "true" else False),
        'otc': (lambda x: True if x.lower() == "true" else False)
        # 'deviceid': (lambda x: parseInt(x))
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
@click.option('--device-table-name', '-d', required=True, type=click.STRING, help="")
@click.option('--identifier-table-name', '-i', required=True, type=click.STRING, help="")
@click.option('--contact-table-name', '-c', required=True, type=click.STRING, help="")
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="")
@click.pass_context
def populate_device_master_refresh(ctx, target_table_name, device_table_name, identifier_table_name, contact_table_name):
    """
    Create device master refresh table
    """
    logger.info(f"Populating device master refresh table '{target_table_name}'...")
    print(target_table_name)
    loader = ctx.obj['loader']

    q = POPULATE_DEVICE_REFRESH_TABLE.format(table_name=target_table_name, device_table_name=device_table_name,
                        identifier_table_name=identifier_table_name, contact_table_name=contact_table_name)

    logger.debug(q)
    loader._submit_single_q(q)
    logger.info("Finished.")


# do all
refresh.add_command(ndc)

# load NDC refresh tables individually
refresh.add_command(ndc_load_indications)
refresh.add_command(ndc_load_orange)
refresh.add_command(ndc_create_tables)

# Load medical device refresh table
refresh.add_command(load_medical_device)
refresh.add_command(populate_device_master_refresh)
