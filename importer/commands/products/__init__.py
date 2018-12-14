import click
import logging

from importer.commands.products.product import product
from importer.commands.products.ndc import ndc
from importer.commands.products.device import device

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def products(ctx): #, batch_size, throttle_size, throttle_time):
    ctx.ensure_object(dict)

products.add_command(product)
products.add_command(ndc)
products.add_command(device)
