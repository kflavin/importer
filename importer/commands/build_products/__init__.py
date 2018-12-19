import click
import logging

from importer.commands.build_products.product import product
from importer.commands.build_products.ndc import ndc
from importer.commands.build_products.device import device

logger = logging.getLogger(__name__)

@click.group()
# @click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
# @click.option('--throttle-size', type=click.INT, default=10000, help="Sleep after this many inserts.")
# @click.option('--throttle-time', type=click.INT, default=3, help="Time (s) to sleep after --throttle-size.")
@click.pass_context
def build_products(ctx): #, batch_size, throttle_size, throttle_time):
    # ctx.ensure_object(dict)
    # ctx.obj['batch_size'] = batch_size
    # ctx.obj['throttle_size'] = throttle_size
    # ctx.obj['throttle_time'] = throttle_time
    pass

# products.add_command(product)
# products.add_command(ndc)
build_products.add_command(device)
# build_products.add_command(product)