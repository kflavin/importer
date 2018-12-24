import click
import logging

from importer.commands.products.product import product
from importer.commands.products.ndc import ndc
from importer.commands.products.device import device
from importer.commands.products.delta import delta

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def products(ctx): #, batch_size, throttle_size, throttle_time):
    ctx.ensure_object(dict)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
@click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
@click.option('--encoding', default="utf-8", type=click.STRING, help="Encoding of CSV file.  default: utf-8")
def preprocess(infile, outfile, encoding):
    loader = BaseLoader()
    loader.preprocess(infile, outfile, encoding)
    print(outfile)


products.add_command(product)
products.add_command(ndc)
products.add_command(device)
products.add_command(delta)
