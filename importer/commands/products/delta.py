
import click
import os
import logging

from importer.loaders.products.delta import DeltaBaseLoader
from importer.loaders.base import BaseLoader

# Common queries
from importer.sql import DELETE_Q
from importer.sql.products.delta.common import RETRIEVE_PRODUCTMASTER_Q

# NDC to Product master queries
from importer.sql.products.delta.ndc_productmaster import (
    DELTA_NDC_TO_PRODUCTMASTER_Q, 
    RETRIEVE_NDC_GROUPS_Q,
    INSERT_PRODUCTMASTER_Q,
    ARCHIVE_PRODUCTMASTER_Q,
    MASTER_IDS_TO_NDC_TABLE)

# Product master to product queries
from importer.sql.products.delta.productmaster_product import (
    DELTA_PRODUCTMASTER_TO_PRODUCT_Q,
    RETRIEVE_PRODUCT_Q,
    INSERT_PRODUCT_Q,
    ARCHIVE_PRODUCT_Q)

# NDC to NDC queries
from importer.sql.products.delta.ndc_to_ndc import (
    DELTA_NDC_TO_NDC_Q,
    RETRIEVE_NDC_Q,
    INSERT_NDC_Q,
    ARCHIVE_NDC_Q)

# DeviceMaster to DeviceMaster queries
from importer.sql.products.delta.devicemaster_devicemaster import (
    DELTA_DEVICEMASTER_DEVICEMASTER_Q,
    RETRIEVE_DEVICEMASTER_Q,
    INSERT_DEVICEMASTER_Q,
    ARCHIVE_DEVICEMASTER_Q
    )

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

# Task 4.1.9
@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def ndc_to_ndc(ctx, left_table_name, right_table_name, right_table_name_archive):

    # we haven't populated master_id at this point.  NDC's are unique on productndc, and related indications (1.1.1)
    join_columns = ["productndc", "ind_name", "te_code", "ind_detailedstatus"]
    # compare_columns = ["master_id", "proprietaryname", "nonproprietaryname"]
    compare_columns = [
        "labelername", "productndc", "proprietaryname", "nonproprietaryname", "producttypename", "marketingcategoryname", "te_code", "type", "ndc_exclude_flag", "drug_id", "ind_drug_name", "ind_name", "status", "phase", "ind_detailedstatus"
        # Excluding definition and interpretation, b/c I'm not populating those yet.
        # "master_id", "labelername", "productndc", "proprietaryname", "nonproprietaryname", "producttypename", "marketingcategoryname", "definition", "te_code", "type", "interpretation", "ndc_exclude_flag", "drug_id", "ind_drug_name", "ind_name", "status", "phase", "ind_detailedstatus"
    ]
    extra_lcols = []
    insert_new_columns = ["labelername", "productndc", "proprietaryname", "nonproprietaryname", "producttypename", "marketingcategoryname", "definition", "te_code", "type", "interpretation", "ndc_exclude_flag", "drug_id", "ind_drug_name", "ind_name", "status", "phase", "ind_detailedstatus"]
    
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
        "ARCHIVE_Q": ARCHIVE_NDC_Q,
        "INSERT_Q": INSERT_NDC_Q,
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
    loader.time = ctx.obj['time']
    loader.delta_table(ctx.obj['do_updates'], ctx.obj['do_inserts'])




# 4.1.11
@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def ndc_to_product_master(ctx, left_table_name, right_table_name, right_table_name_archive):

    # join_columns = ["master_id"]
    join_columns = ["proprietaryname", "nonproprietaryname"]
    compare_columns = ["proprietaryname", "nonproprietaryname"]
    # extra_lcols = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    extra_lcols = []    # additional columns from left table, use for UPDATEs
    insert_new_columns = ["proprietaryname", "nonproprietaryname"]  # columns to use for INSERTs
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
        "DELTA_Q": DELTA_NDC_TO_PRODUCTMASTER_Q,
        "RETRIEVE_LEFT_Q": RETRIEVE_NDC_GROUPS_Q,
        "RETRIEVE_RIGHT_Q": RETRIEVE_PRODUCTMASTER_Q,
        "DELETE_Q": DELETE_Q,
        "ARCHIVE_Q": ARCHIVE_PRODUCTMASTER_Q,
        "INSERT_Q": INSERT_PRODUCTMASTER_Q,
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
    compare_columns = ["master_id", "master_type", "proprietaryname", "nonproprietaryname"]
    # extra_lcols = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    extra_lcols = []
    extra_rcols = [
        "id",
        "client_product_id",
        "master_id",
        "master_type",
        "proprietaryname",
        "nonproprietaryname",
        "drug_type",
        "source_type",
        "company_id",
        "is_admin_approved",
        "verified_source",
        "verified_source_id",
        "created_by",
        "created_date",
        "modified_by",
        "modified_date",
        "eff_date",
        "end_date"
    ]
    insert_new_columns = ["master_id", "proprietaryname", "nonproprietaryname"] # columns to use for new rows
    
    # xform_left = {
    #     "proprietaryname": (lambda x: x.upper() if x else None),
    #     "nonproprietaryname": (lambda x: x.upper() if x else None)
    # }
    xform_left = {}
    xform_right = {}
    
    if not right_table_name_archive:
        right_table_name_archive = f"{right_table_name}_archive"

    loader_args = {
        "DELTA_Q": DELTA_PRODUCTMASTER_TO_PRODUCT_Q,
        "RETRIEVE_LEFT_Q": RETRIEVE_PRODUCTMASTER_Q,
        "RETRIEVE_RIGHT_Q": RETRIEVE_PRODUCT_Q,
        "DELETE_Q": DELETE_Q,
        "ARCHIVE_Q": ARCHIVE_PRODUCT_Q,
        "INSERT_Q": INSERT_PRODUCT_Q,
        "join_columns": join_columns,
        "compare_columns": compare_columns,
        "extra_lcols": extra_lcols,
        "extra_rcols": extra_rcols,
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

# Add the master_id back to NDC table.  Task 4.1.11
@click.command()
@click.option('--ndc-table-name', '-n', required=True, type=click.STRING, help="")
@click.option('--product-table-name', '-p', required=True, type=click.STRING, help="")
@click.pass_context
def masterid_to_ndc(ctx, ndc_table_name, product_table_name):
    query = MASTER_IDS_TO_NDC_TABLE.format(ndc_table_name=ndc_table_name, product_table_name=product_table_name)
    loader = BaseLoader()
    loader.connect(**ctx.obj['db_credentials'])
    loader._submit_single_q(query)

# 4.2.5
@click.command()
@click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
@click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
@click.option('--right-table-name-archive', '-a', type=click.STRING, help="")
@click.pass_context
def devicemaster_devicemaster(ctx, left_table_name, right_table_name, right_table_name_archive):

    # join_columns = ["master_id"]
    join_columns = ["deviceid"]
    compare_columns = [
        "deviceid", "publicdevicerecordkey", "deviceidtype", "devicedescription", "companyname", "phone", "phoneextension", "email", "brandname", "dunsnumber", "deviceidissuingagency", "containsdinumber", "pkgquantity", "pkgdiscontinuedate", "pkgstatus", "pkgtype", "rx", "otc"
    ]
    # extra_lcols = ["id", "master_id", "proprietaryname", "nonproprietaryname"]
    extra_lcols = []    # additional columns from left table, use for UPDATEs
    insert_new_columns = ["deviceid", "publicdevicerecordkey", "deviceidtype", "devicedescription", "companyname", "phone", "phoneextension", "email", "brandname", "dunsnumber", "deviceidissuingagency", "containsdinumber", "pkgquantity", "pkgdiscontinuedate", "pkgstatus", "pkgtype", "rx", "otc"]

    xform_left = {
        # "proprietaryname": (lambda x: x.upper() if x else None),
        # "nonproprietaryname": (lambda x: x.upper() if x else None)
    }
    xform_right = {}

    if not right_table_name_archive:
        right_table_name_archive = f"{right_table_name}_archive"

    loader_args = {
        "DELTA_Q": DELTA_DEVICEMASTER_DEVICEMASTER_Q,
        "RETRIEVE_LEFT_Q": RETRIEVE_DEVICEMASTER_Q,
        "RETRIEVE_RIGHT_Q": RETRIEVE_DEVICEMASTER_Q,
        "DELETE_Q": DELETE_Q,
        "ARCHIVE_Q": ARCHIVE_PRODUCTMASTER_Q,
        "INSERT_Q": INSERT_PRODUCTMASTER_Q,
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


delta.add_command(ndc_to_ndc)
delta.add_command(ndc_to_product_master)
delta.add_command(productmaster_to_product)
delta.add_command(masterid_to_ndc)
delta.add_command(devicemaster_devicemaster)
