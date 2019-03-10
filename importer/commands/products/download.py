import click
import os
import logging

from importer.downloaders.products.product_downloader import ProductDownloader

logger = logging.getLogger(__name__)

# Hardcoding these.  If they change, the scraping will probably break anyways.
drugbank_url = "https://www.drugbank.ca/releases/latest#open-data"
indications_url = "https://chiragjp.shinyapps.io/repoDB"
cms_url = "https://www.cms.gov/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/List_of_Codes.html"
gudid_url = "https://accessgudid.nlm.nih.gov/download"
ndc_url = "https://www.fda.gov/drugs/informationondrugs/ucm142438.htm"
orange_url = "https://www.fda.gov/drugs/informationondrugs/ucm129662.htm"

@click.group()
@click.pass_context
def download(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="drugbank", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def drugbank(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_drugbank(drugbank_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="indications", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def indications(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_indications(indications_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="cms", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def cms(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_cms(cms_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="gudid", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def gudid(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_gudid(gudid_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="ndc", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def ndc(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_ndc(ndc_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.option('--prefix', '-p', required=False, default="orangebook", type=click.STRING, help="S3 prefix, no trailing slash.")
@click.pass_context
def orangebook(ctx, bucket, prefix):
    d = ProductDownloader()
    check_result(d.dl_orange(orange_url, bucket, prefix))

@click.command()
@click.option('--bucket', '-b', required=False, default="rxvproduct", type=click.STRING, help="S3 bucket")
@click.pass_context
def all(ctx, bucket):
    d = ProductDownloader()
    check_result(d.dl_drugbank(drugbank_url, bucket, "drugbank"))
    check_result(d.dl_indications(indications_url, bucket, "indications"))
    check_result(d.dl_cms(cms_url, bucket, "cms"))
    check_result(d.dl_gudid(gudid_url, bucket, "gudid"))
    check_result(d.dl_ndc(ndc_url, bucket, "ndc"))
    check_result(d.dl_orange(orange_url, bucket, "orangebook"))

def check_result(result):
    if result:
        logger.info("Download complete.")
    else:
        logger.info("Download failed.")

download.add_command(drugbank)
download.add_command(indications)
download.add_command(cms)
download.add_command(gudid)
download.add_command(ndc)
download.add_command(orangebook)
download.add_command(all)
