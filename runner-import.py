#!/usr/bin/env python3
import click
import os
import logging
import sys
# import warnings
# warnings.filterwarnings("ignore", category=RuntimeWarning, module=".*pandas.*|.*numpy.*")

# Ignore numpy warnings, which are due to other packages being compiled against an earlier numpy.
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

from importer.loggers.cloudwatch_handler import CloudWatchLogHandler
from importer.commands.npi import npi
from importer.commands.products import products
from importer.commands.gudid.main import gudid
# from importer.commands.build_products import build_products

from importer.commands.tools import tools

@click.group()
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
@click.option('--throttle-size', type=click.INT, default=10000, help="Sleep after this many inserts.")
@click.option('--throttle-time', type=click.INT, default=3, help="Time (s) to sleep after --throttle-size.")
@click.option('--debug/--no-debug', default=False)
@click.option('--warnings/--no-warnings', default=True)
@click.option('--logs', '-l', default="system", type=click.Choice(["cloudwatch", "system"]), help="[cloudwatch|system] - CW requires aws_region/instance_id env vars to be set.")
@click.option('--log-group', default="importer-test", help="Cloudwatch log group name")
@click.option('--time/--no-time', default=False, help="Print times.")
@click.pass_context
def start(ctx, batch_size, throttle_size, throttle_time, debug, warnings, logs, log_group, time):
    ctx.ensure_object(dict)
    logger = logging.getLogger("importer")

    ctx.obj['batch_size'] = batch_size
    ctx.obj['throttle_size'] = throttle_size
    ctx.obj['throttle_time'] = throttle_time
    ctx.obj['warnings'] = warnings
    ctx.obj['time'] = time

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
        sh = logging.StreamHandler(sys.stdout)

        # Add a formatter with time.
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:  %(message)s", "%Y-%m-%d %H:%M:%S")
        sh.setFormatter(formatter)

        sh.setLevel(handler_level)
        logger.addHandler(sh)
        logger.info("Sending runner logs to stdout")

    logger.debug("DEBUGGING IS ENABLED")

    ctx.obj['db_credentials'] = {
        'user': os.environ.get('db_user'),
        'password': os.environ.get('db_password'),
        'host': os.environ.get('db_host'),
        'database': os.environ.get('db_schema'),
        'debug': debug
    }
    
        
start.add_command(npi)
# start.add_command(hdm)
# start.add_command(product)
start.add_command(products)
start.add_command(tools)
start.add_command(gudid)

# Deprecated
# start.add_command(build_products)

if __name__ == '__main__':
    start()

