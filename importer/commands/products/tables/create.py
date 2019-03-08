import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql.products.refresh.tables import (CREATE_INDICATIONS_DDL, CREATE_NDC_DDL,
    CREATE_MARKETING_DDL, CREATE_ORANGE_DDL, CREATE_MEDICAL_DEVICE_MASTER_DDL,
    CREATE_GUDID_CONTACTS_DDL, CREATE_GUDID_DEVICES_DDL, CREATE_GUDID_IDENTIFERS_DDL)
from importer.sql.products.product import (CREATE_PRODUCT_DDL, CREATE_PRODUCT_MASTER_DDL)
from importer.sql.products.device import CREATE_DEVICEMASTER_DDL
from importer.sql.products.ndc import CREATE_NDCMASTER_DDL
from importer.sql.base import (CREATE_TABLE_LIKE_IFNE_DDL)

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def create(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_indications(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_INDICATIONS_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_ndc(ctx, table_name):
    archive_table_name = table_name+"_archive"
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_NDC_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_marketing(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_MARKETING_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_orange(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_ORANGE_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_medicaldevice(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_MEDICAL_DEVICE_MASTER_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_gudid_contacts(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q1 = CREATE_GUDID_CONTACTS_DDL.format(table_name=table_name)
    loader._query(q1)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_gudid_devices(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q2 = CREATE_GUDID_DEVICES_DDL.format(table_name=table_name)
    loader._query(q2)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def refresh_gudid_identifers(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q1 = CREATE_GUDID_IDENTIFERS_DDL.format(table_name=table_name)
    loader._query(q1)

@click.command()
@click.pass_context
def refresh_gudid_all(ctx):
    """
    Create all gudid refresh tables
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    guid_devices = "refresh_gudid_devices"
    guid_identifiers = "refresh_gudid_identifiers"
    guid_contacts = "refresh_gudid_contacts"
    q6 = CREATE_GUDID_CONTACTS_DDL.format(table_name=guid_contacts)
    q7 = CREATE_GUDID_DEVICES_DDL.format(table_name=guid_devices)
    q8 = CREATE_GUDID_IDENTIFERS_DDL.format(table_name=guid_identifiers)
    loader._query(q6)
    loader._query(q7)
    loader._query(q8)
    print("Created all gudid tables")


@click.command()
@click.pass_context
def refresh_all(ctx):
    """
    Create all refresh tables using default names.
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    indications_table_name = "refresh_indications"
    ndc_table_name = "refresh_ndc_product"
    marketing_table_name = "refresh_marketing_codes"
    orange_table_name = "refresh_orange"
    device_master_table_name = "refresh_devicemaster"

    q1 = CREATE_INDICATIONS_DDL.format(table_name=indications_table_name)
    q2 = CREATE_NDC_DDL.format(table_name=ndc_table_name)
    q3 = CREATE_MARKETING_DDL.format(table_name=marketing_table_name)
    q4 = CREATE_ORANGE_DDL.format(table_name=orange_table_name)
    # q5 = CREATE_MEDICAL_DEVICE_MASTER_DDL.format(table_name=device_master_table_name)
    loader._query(q1)
    loader._query(q2)
    loader._query(q3)
    loader._query(q4)
    # loader._query(q5)

    guid_devices = "refresh_gudid_devices"
    guid_identifiers = "refresh_gudid_identifiers"
    guid_contacts = "refresh_gudid_contacts"
    q6 = CREATE_GUDID_CONTACTS_DDL.format(table_name=guid_contacts)
    q7 = CREATE_GUDID_DEVICES_DDL.format(table_name=guid_devices)
    q8 = CREATE_GUDID_IDENTIFERS_DDL.format(table_name=guid_identifiers)
    loader._query(q6)
    loader._query(q7)
    loader._query(q8)

    print(f"CREATED refresh tables:\n {indications_table_name}\n {ndc_table_name}\n {marketing_table_name}\n {orange_table_name}")
    print("...and gudid tables")

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def prod_productmaster(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_PRODUCT_MASTER_DDL.format(table_name=table_name)
    loader._query(q)

    product_master_archive_table_name = table_name + "_archive"

    # Create archive table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=product_master_archive_table_name, source_table_name=table_name)
    )

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def prod_product(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_PRODUCT_DDL.format(table_name=table_name)
    loader._query(q)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def prod_ndc(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    q = CREATE_NDCMASTER_DDL.format(table_name=table_name)
    loader._query(q)

    archive_table_name = table_name + "_archive"
    stage_table_name = table_name + "_stage"

    # Create NDC archive table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=archive_table_name, source_table_name=table_name)
    )

    # Create NDC stage table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=stage_table_name, source_table_name=table_name)
    )

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def prod_devicemaster(ctx, table_name):
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    archive_table_name = table_name + "_archive"
    stage_table_name = table_name + "_stage"

    q = CREATE_DEVICEMASTER_DDL.format(table_name=table_name)
    loader._query(q)

    # Create Device Master archive table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=archive_table_name, source_table_name=table_name)
    )

    # Create Device Master stage table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=stage_table_name, source_table_name=table_name)
    )
    print("Created device master tables.")

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table to create.")
@click.pass_context
def prod_service(ctx, table_name):
    print("Not implemented.")
    pass

@click.command()
@click.pass_context
def prod_all(ctx):
    """
    Create all production tables using default names.
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])

    product_table_name = "product"
    productmaster_table_name = "product_master"
    devicemaster_table_name = "device_master"
    servicemaster_table_name = "service_master"
    ndcmaster_table_name = "ndc_master"

    q1 = CREATE_PRODUCT_DDL.format(table_name=product_table_name)
    q2 = CREATE_PRODUCT_MASTER_DDL.format(table_name=productmaster_table_name)
    q3 = CREATE_DEVICEMASTER_DDL.format(table_name=devicemaster_table_name)
    q4 = CREATE_NDCMASTER_DDL.format(table_name=ndcmaster_table_name)
    # q5 = CREATE_SERVICE_MASTER_DDL.format(table_name=servicemaster_table_name)
    loader._query(q1)
    loader._query(q2)
    loader._query(q3)
    loader._query(q4)
    # loader._query(q5)

    product_master_archive_table_name = productmaster_table_name + "_archive"
    ndc_archive_table_name = ndcmaster_table_name + "_archive"
    devicemaster_archive_table_name = devicemaster_table_name + "_archive"
    ndc_stage_table_name = ndcmaster_table_name + "_stage"
    devicemaster_stage_table_name = devicemaster_table_name + "_stage"

    # Create product master archive
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=product_master_archive_table_name, source_table_name=productmaster_table_name)
    )

    # Create NDC master archive
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=ndc_archive_table_name, source_table_name=ndcmaster_table_name)
    )

    # Create Device Master archive table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=devicemaster_archive_table_name, source_table_name=devicemaster_table_name)
    )

    # Create NDC stage table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=ndc_stage_table_name, source_table_name=ndcmaster_table_name)
    )

    # Create Device master stage table
    loader._submit_single_q(
        CREATE_TABLE_LIKE_IFNE_DDL.format(target_table_name=devicemaster_stage_table_name, source_table_name=devicemaster_table_name)
    )
    print("Create prod tables")

# refresh tables
create.add_command(refresh_indications)
create.add_command(refresh_ndc)
create.add_command(refresh_marketing)
create.add_command(refresh_orange)
create.add_command(refresh_medicaldevice)
create.add_command(refresh_all)
create.add_command(refresh_gudid_all)
create.add_command(refresh_gudid_contacts)
create.add_command(refresh_gudid_devices)
create.add_command(refresh_gudid_identifers)

# prod tables
create.add_command(prod_product)
create.add_command(prod_productmaster)
create.add_command(prod_ndc)
create.add_command(prod_devicemaster)
create.add_command(prod_service)
create.add_command(prod_all)