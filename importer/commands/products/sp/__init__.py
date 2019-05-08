import click
import logging
import os

def load_sql(fileName):
    return open(os.path.join(SP_DIR, fileName + ".sql")).read()

def recreate_sp(sql_file_name, loader, user, database, prod_db):
    sp_name = sql_file_name
    q = load_sql(sql_file_name).format(database=database, user=user, prod_db=prod_db)
    drop_q = DROP_SP.format(database=database, procedure_name=sp_name)
    logger.debug(drop_q)
    logger.debug(q)
    logger.info(f"Recreating {database}.{sp_name}")
    # logger.info(f"Dropping {database}.{sp_name}")
    loader._query(drop_q)
    # logger.info(f"Creating {database}.{sp_name}")
    loader._query(q)

from importer.loaders.base import BaseLoader
from importer.commands.products.sp.create import create
from importer.commands.products.sp.drop import drop
from importer.sql.products.sp.drop import DROP_SP
from importer import SP_DIR

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
        logger.debug(f"Using db {ctx.obj['db_name']}")
    else:
        ctx.obj['db_name'] = db_name

sp.add_command(create)
sp.add_command(drop)