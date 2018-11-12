#!/usr/bin/env python3
import click
import os
import logging, watchtower

from importer.loggers.cloudwatch import CloudWatchLogger
from importer.commands.npi import npi

# DEBUG = False
# logger = None

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--logs', '-l', default="system", type=click.Choice(["cloudwatch", "system"]), help="[cloudwatch|system] - CW requires aws_region/instance_id env vars to be set.")
def start(debug, logs):
    DEBUG = debug
    # global logger
    
    if logs == "cloudwatch":
        # region = os.environ.get('aws_region')
        # logger = CloudWatchLogger("importer-npi", os.environ.get('instance_id'), region=region)
        # logger.info("Sending runner logs to cloudwatch")

        # logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("importer")
        logger.addHandler(watchtower.CloudWatchLogHandler(log_group="importer-test", stream_name=os.environ.get('instance_id')))
        #logger.addHandler(watchtower.CloudWatchLogHandler())
        logger.info("Sending runner logs to cloudwatch")
    else:
        logger = logging.getLogger("importer")
        logger.setLevel(level="INFO")
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)
        # fh = logging.FileHandler("importer.log")
        # fh.setLevel(logging.DEBUG)
        # logger.addHandler(fh)
        logger.info("Sending runner logs to stdout")
    
        
start.add_command(npi)

if __name__ == '__main__':
    start()

