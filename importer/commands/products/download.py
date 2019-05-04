import click
import os
import traceback
import logging

from importer.downloaders.products.product_downloader import ProductDownloader

logger = logging.getLogger(__name__)

# Hardcoding these.  If they change, the scraping will probably break anyways.
drugbank_url = "https://www.drugbank.ca/releases/latest#open-data"
indications_url = "https://chiragjp.shinyapps.io/repoDB"
cms_url = "https://www.cms.gov/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/List_of_Codes.html"
# gudid_url = "https://accessgudid.nlm.nih.gov/download"
gudid_url = "https://accessgudid.nlm.nih.gov/download/delimited"        # we want the delimited files
ndc_url = "https://www.fda.gov/drugs/informationondrugs/ucm142438.htm"
# orange_url = "https://www.fda.gov/drugs/informationondrugs/ucm129662.htm"
orange_url = "https://www.fda.gov/media/76860/download"  # New URL as of 5/2/2019
marketing_codes_url = "https://www.fda.gov/forindustry/datastandards/structuredproductlabeling/ucm162528.htm"

s3_bucket_name = "rxv-product-data"

@click.group()
@click.pass_context
def download(ctx):
    """
    Download product data files.  File names are created with the current day, and then copied to a "latest" file.  Subsequent runs
    will overwrite the "latest" file, but skip duplicate "date" files.
    """
    ctx.ensure_object(dict)

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="drugbank", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def drugbank(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_drugbank(drugbank_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="indications", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def indications(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_indications(indications_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="cms", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def cms(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_cms(cms_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="gudid", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def gudid(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_gudid(gudid_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="ndc", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def ndc(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_ndc(ndc_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="orangebook", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def orangebook(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_orange(orange_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="marketingcodes", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def marketingcodes(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_marketing_codes(marketing_codes_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default=s3_bucket_name, type=click.STRING, help="S3 bucket")
@click.pass_context
def all(ctx, bucket):
    d = ProductDownloader()
    try:
        check_result(d.dl_drugbank(drugbank_url, bucket, "drugbank"))
    except Exception as e:
        handle_exception(e, "Failed to download drug bank file but continuing...")

    try:
        check_result(d.dl_indications(indications_url, bucket, "indications"))
    except Exception as e:
        handle_exception(e, "Failed to download indications file but continuing...")

    try:
        check_result(d.dl_cms(cms_url, bucket, "cms"))
    except Exception as e:
        handle_exception(e, "Failed to download cms file but continuing...")

    try:
        check_result(d.dl_gudid(gudid_url, bucket, "gudid"))
    except Exception as e:
        handle_exception(e, "Failed to download gudid file but continuing...")
    
    try:
        check_result(d.dl_ndc(ndc_url, bucket, "ndc"))
    except Exception as e:
        handle_exception(e, "Failed to download NDC file but continuing...")

    try:
        check_result(d.dl_orange(orange_url, bucket, "orangebook"))
    except Exception as e:
        handle_exception(e, "Failed to download Orangebook file but continuing...")

    try:
        check_result(d.dl_marketing_codes(marketing_codes_url, bucket, "marketingcodes"))
    except Exception as e:
        handle_exception(e, "Failed to download marketing codes file but continuing...")

def check_result(result):
    if result:
        logger.info("Download complete.")
    else:
        logger.info("Download failed.")

def handle_exception(e, msg):
    logger.warn(traceback.format_exc())
    logger.warn(e)
    logger.warn(msg)

download.add_command(drugbank)
download.add_command(indications)
download.add_command(cms)
download.add_command(gudid)
download.add_command(ndc)
download.add_command(orangebook)
download.add_command(marketingcodes)
download.add_command(all)
