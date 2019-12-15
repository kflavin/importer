import click
import logging

from importer.commands.products.commands import product
from importer.commands.products.ndc import ndc
from importer.commands.products.device import device
from importer.commands.products.delta import delta
from importer.commands.products.merge import merge
from importer.commands.products.tables import tables
from importer.commands.products.sp import sp
from importer.commands.products.refresh import refresh
from importer.commands.products.download import download

logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def products(ctx): #, batch_size, throttle_size, throttle_time):
    ctx.ensure_object(dict)


products.add_command(product)
products.add_command(ndc)
products.add_command(device)
products.add_command(delta)
products.add_command(merge)
products.add_command(tables)
products.add_command(refresh)
products.add_command(download)
products.add_command(sp)
