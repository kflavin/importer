
import click
import os
import logging

from importer.loaders.products.base import DeltaBaseLoader
from importer.sql import DELETE_Q
from importer.sql.products.delta.ndc_productmaster import (
    DELTA_NDC_TO_PRODUCT_MASTER_Q as DELTA_Q, 
    RETRIEVE_NDC_Q as RETRIEVE_LEFT_Q,
    RETRIEVE_PRODUCT_MASTER_Q as RETRIEVE_RIGHT_Q,
    INSERT_PRODUCT_MASTER_Q as INSERT_Q,
    ARCHIVE_PRODUCT_MASTER_Q as ARCHIVE_Q)

from importer.sql.products.delta.productmaster_product import (
    DELTA_PRODUCTMASTER_TO_PRODUCT_Q,
    RETRIEVE_PRODUCT_Q,
    RETRIEVE_PRODUCTMASTER_Q)

from importer.sql.products.delta.ndc_to_ndc import (
    DELTA_NDC_TO_NDC_Q, RETRIEVE_NDC_Q)

logger = logging.getLogger(__name__)

@click.group()
@click.option('--only-new-records', '-n', default=False, type=click.STRING, help="")
@click.option('--only-updates', '-u', default=False, type=click.STRING, help="")
@click.pass_context
def delta(ctx, only_new_records, only_updates):
    ctx.ensure_object(dict)
    if only_new_records and not only_updates:
        ctx.obj['do_inserts'] = True
        ctx.obj['do_updates'] = False
    elif not only_new_records and only_updates:
        ctx.obj['do_inserts'] = False
        ctx.obj['do_updates'] = True
    else:
        ctx.obj['do_inserts'] = ctx.obj['do_updates'] = True

# Task 4.1.1.9
@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def ndc_to_ndc(ctx, left_table_name, right_table_name, right_table_name_archive):

    join_columns = ["productndc", "ind_name", "te_code", "ind_detailedstatus"]
    # compare_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    compare_columns = [
        "master_id", "labelername", "productndc", "proprietaryname", "nonproprietaryname", "producttypename", "marketingcategoryname", "te_code", "type", "ndc_exclude_flag", "drug_id", "ind_drug_name", "ind_name", "status", "phase", "ind_detailedstatus"
        # Excluding definition and interpretation, b/c I'm not populating those yet.
        # "master_id", "labelername", "productndc", "proprietaryname", "nonproprietaryname", "producttypename", "marketingcategoryname", "definition", "te_code", "type", "interpretation", "ndc_exclude_flag", "drug_id", "ind_drug_name", "ind_name", "status", "phase", "ind_detailedstatus"
    ]
    extra_lcols = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    insert_new_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    
    xform_left = {
        # "proprietaryname": (lambda x: x.upper() if x else None),
        # "nonproprietaryname": (lambda x: x.upper() if x else None)
    }
    xform_right = {}

    if not right_table_name_archive:
        right_table_name_archive = f"{right_table_name}_archive"

    loader_args = {
        "DELTA_Q": DELTA_NDC_TO_NDC_Q,
        "RETRIEVE_LEFT_Q": RETRIEVE_NDC_Q,
        "RETRIEVE_RIGHT_Q": RETRIEVE_NDC_Q,
        "DELETE_Q": DELETE_Q,
        "ARCHIVE_Q": ARCHIVE_Q,
        "INSERT_Q": INSERT_Q,
        "join_columns": join_columns,
        "compare_columns": compare_columns,
        "extra_lcols": extra_lcols,
        "insert_new_columns": insert_new_columns,
        "xform_left": xform_left,
        "xform_right": xform_right,
        "left_table_name": left_table_name,
        "right_table_name": right_table_name,
        "right_table_name_archive": right_table_name_archive,
        "warnings": ctx.obj['warnings'], 
        "batch_size": ctx.obj['batch_size'], 
        "throttle_size": ctx.obj['throttle_size'], 
        "throttle_time": ctx.obj['throttle_time']
    }

    loader = DeltaBaseLoader(**loader_args)
    loader.connect(**ctx.obj['db_credentials'])
    loader.delta_table(ctx.obj['do_updates'], ctx.obj['do_inserts'])

@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def ndc_to_product_master(ctx, left_table_name, right_table_name, right_table_name_archive):

    join_columns = ["master_id"]
    compare_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    extra_lcols = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    insert_new_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    # Upper case the second and third columns (proprietary and non-proprietary name)
    # left_table_transforms = {
    #     2: (lambda x: x.upper() if x else None),
    #     3: (lambda x: x.upper() if x else None)
    # }
    xform_left = {
        "proprietaryname": (lambda x: x.upper() if x else None),
        "nonproprietaryname": (lambda x: x.upper() if x else None)
    }
    xform_right = {}

    if not right_table_name_archive:
        right_table_name_archive = f"{right_table_name}_archive"

    loader_args = {
        "DELTA_Q": DELTA_Q,
        "RETRIEVE_LEFT_Q": RETRIEVE_LEFT_Q,
        "RETRIEVE_RIGHT_Q": RETRIEVE_RIGHT_Q,
        "DELETE_Q": DELETE_Q,
        "ARCHIVE_Q": ARCHIVE_Q,
        "INSERT_Q": INSERT_Q,
        "join_columns": join_columns,
        "compare_columns": compare_columns,
        "extra_lcols": extra_lcols,
        "insert_new_columns": insert_new_columns,
        "xform_left": xform_left,
        "xform_right": xform_right,
        "left_table_name": left_table_name,
        "right_table_name": right_table_name,
        "right_table_name_archive": right_table_name_archive,
        "warnings": ctx.obj['warnings'], 
        "batch_size": ctx.obj['batch_size'], 
        "throttle_size": ctx.obj['throttle_size'], 
        "throttle_time": ctx.obj['throttle_time']
    }

    loader = DeltaBaseLoader(**loader_args)
    loader.connect(**ctx.obj['db_credentials'])
    loader.delta_table(ctx.obj['do_updates'], ctx.obj['do_inserts'])


# Not working yet.  Task 4.1.12
@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def productmaster_to_product(ctx, left_table_name, right_table_name, right_table_name_archive):

    join_columns = ["master_id", "master_type"]
    compare_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    extra_lcols = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    insert_new_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    # Upper case the second and third columns (proprietary and non-proprietary name)
    # left_table_transforms = {
    #     2: (lambda x: x.upper() if x else None),
    #     3: (lambda x: x.upper() if x else None)
    # }
    xform_left = {
        "proprietaryname": (lambda x: x.upper() if x else None),
        "nonproprietaryname": (lambda x: x.upper() if x else None)
    }
    xform_right = {}
    
    if not right_table_name_archive:
        right_table_name_archive = f"{right_table_name}_archive"

    loader_args = {
        "DELTA_Q": DELTA_PRODUCTMASTER_TO_PRODUCT_Q,
        "RETRIEVE_LEFT_Q": RETRIEVE_PRODUCTMASTER_Q,
        "RETRIEVE_RIGHT_Q": RETRIEVE_PRODUCT_Q,
        "DELETE_Q": DELETE_Q,
        "ARCHIVE_Q": ARCHIVE_Q,
        "INSERT_Q": INSERT_Q,
        "join_columns": join_columns,
        "compare_columns": compare_columns,
        "extra_lcols": extra_lcols,
        "insert_new_columns": insert_new_columns,
        "xform_left": xform_left,
        "xform_right": xform_right,
        "left_table_name": left_table_name,
        "right_table_name": right_table_name,
        "right_table_name_archive": right_table_name_archive,
        "warnings": ctx.obj['warnings'], 
        "batch_size": ctx.obj['batch_size'], 
        "throttle_size": ctx.obj['throttle_size'], 
        "throttle_time": ctx.obj['throttle_time']
    }

    loader = DeltaBaseLoader(**loader_args)
    loader.connect(**ctx.obj['db_credentials'])
    loader.delta_table(ctx.obj['do_updates'], ctx.obj['do_inserts'])

delta.add_command(ndc_to_product_master)
delta.add_command(productmaster_to_product)
delta.add_command(ndc_to_ndc)