import click
import os
import logging
import html

from importer.loaders.build_products.device import MedDeviceCompleteLoader, BaseLoader, convert_date
from importer.sql import (INSERT_QUERY)
from importer.sql.products.refresh.ndc import (REFRESH_NDC_TABLE_DDL, REFRESH_NDC_TABLE_LOAD_INDICATIONS, 
                    REFRESH_NDC_TABLE_DDL, REFRESH_NDC_TABLE_LOAD_ORANGE)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def refresh(ctx):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader

@click.command()
@click.option('--target-table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--source-table-name', '-s', required=True, type=click.STRING, help="")
@click.option('--indications-table-name', '-i', required=True, type=click.STRING, help="")
@click.option('--ndc-product-table-name', '-n', required=True, type=click.STRING, help="")
@click.option('--orange-table-name', '-o', required=True, type=click.STRING, help="")
@click.pass_context
def ndc(ctx, target_table_name,
             source_table_name,
             indications_table_name,
             ndc_product_table_name,
             orange_table_name):
    """
    Load the full NDC refresh table.
    """
    loader = ctx.obj['loader']
    target_table_name2 = target_table_name + "2"

    logger.info(f"Creating tables {target_table_name} and {target_table_name2}...")
    q1 = REFRESH_NDC_TABLE_DDL.format(target_table_name=target_table_name, source_table_name=source_table_name)
    q2 = REFRESH_NDC_TABLE_DDL.format(target_table_name=target_table_name2, source_table_name=source_table_name)
    logger.debug(q1)
    logger.debug(q2)
    loader._submit_single_q(q1)
    loader._submit_single_q(q2)
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
    q4 = REFRESH_NDC_TABLE_LOAD_ORANGE.format(target_table_name2=target_table_name2,
                                         target_table_name=target_table_name,
                                         orange_table_name=orange_table_name)
    logger.debug(q4)
    loader._submit_single_q(q4)
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

    logger.info(f"Creating tables {target_table_name} and {target_table_name2}...")
    q1 = REFRESH_NDC_TABLE_DDL.format(target_table_name=target_table_name, source_table_name=source_table_name)
    q2 = REFRESH_NDC_TABLE_DDL.format(target_table_name=target_table_name2, source_table_name=source_table_name)
    logger.debug(q1)
    logger.debug(q2)
    loader._submit_single_q(q1)
    loader._submit_single_q(q2)
    logger.info("Finished.")

refresh.add_command(ndc)
refresh.add_command(ndc_load_indications)
refresh.add_command(ndc_load_orange)
refresh.add_command(ndc_create_tables)
