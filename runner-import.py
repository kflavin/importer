#!/usr/bin/env python3
import click
import os
import logging
# import warnings
# warnings.filterwarnings("ignore", category=RuntimeWarning, module=".*pandas.*|.*numpy.*")

from importer.loggers.cloudwatch_handler import CloudWatchLogHandler
from importer.commands.npi import npi
from importer.commands.products import products

from importer.commands.tools.csv import csv

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--logs', '-l', default="system", type=click.Choice(["cloudwatch", "system"]), help="[cloudwatch|system] - CW requires aws_region/instance_id env vars to be set.")
@click.option('--log-group', default="importer-test", help="Cloudwatch log group name")
@click.pass_context
def start(ctx, debug, logs, log_group):
    ctx.ensure_object(dict)
    logger = logging.getLogger("importer")

    if debug:
        logger.setLevel(level="DEBUG")
        ctx.obj['debug'] = True
        handler_level = logging.DEBUG
    else:
        logger.setLevel(level="INFO")
        ctx.obj['debug'] = False
        handler_level = logging.INFO
    
    if logs == "cloudwatch":
        region = os.environ.get('aws_region')
        logger.addHandler(CloudWatchLogHandler(log_group=log_group, stream_name=os.environ.get('instance_id'), region=region))
        logger.info("Sending runner logs to cloudwatch")
    else:
        sh = logging.StreamHandler()
        sh.setLevel(handler_level)
        logger.addHandler(sh)
        logger.info("Sending runner logs to stdout")
    
        
start.add_command(npi)
# start.add_command(hdm)
# start.add_command(product)
start.add_command(products)
start.add_command(csv)

if __name__ == '__main__':
    start()

