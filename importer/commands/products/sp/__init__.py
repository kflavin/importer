import click
import logging

from importer.loaders.base import BaseLoader
from importer.commands.products.sp.create import create
from importer.commands.products.sp.drop import drop
from importer.commands.products.sp.update import update

logger = logging.getLogger(__name__)

@click.group()
@click.option('--db-name', '-d', required=False, default=None, type=click.STRING, help="")
@click.pass_context
def sp(ctx, db_name): #, batch_size, throttle_size, throttle_time):
    ctx.ensure_object(dict)
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    ctx.obj['loader'] = loader

    if not db_name:
        ctx.obj['db_name'] = loader._query("select database()")[0][0]
        logger.info(f"Using db {ctx.obj['db_name']}")

sp.add_command(create)
sp.add_command(drop)
sp.add_command(update)