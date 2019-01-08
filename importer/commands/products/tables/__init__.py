import click
import logging

from importer.commands.products.tables.create import create
from importer.commands.products.tables.drop import drop

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def tables(ctx): #, batch_size, throttle_size, throttle_time):
    ctx.ensure_object(dict)

tables.add_command(create)
tables.add_command(drop)