import click
import os
import logging

from importer.loaders.base import BaseLoader, convert_date
from importer.sql.products.sp.drop import DROP_SP
from importer.commands.products.sp.common import sp_names

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def drop(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.pass_context
def all(ctx):
    loader = ctx.obj['loader']

    # Drop SP's
    for key,val in sp_names.items():
        loader._query(DROP_SP.format(database=ctx.obj['db_name'], procedure_name=val))

    logger.info("Dropping all SP's")


drop.add_command(all)
