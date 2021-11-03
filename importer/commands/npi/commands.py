import click
import os
import logging

from importer.loaders import NpiLoader

logger = logging.getLogger(__name__)

args = {
    'user': os.environ.get('loader_db_user'),
    'password': os.environ.get('loader_db_password'),
    'host': os.environ.get('loader_db_host'),
    'database': os.environ.get('loader_db_name')
}

# We don't want to use these for the file loading, just fetching files from S3.
extra_args = {
    'dictionary': True,      # For referencing returned columns by name
    'buffered': True         # so we can get row counts without reading every row
}


@click.group()
def npi():
    pass


@click.command()
@click.option('--url-prefix', '-u', required=True, type=click.STRING, help="URL directory that contains weekly or monthly files to fetch")
@click.option('--table-name', '-t', default="npi_import_log", type=click.STRING, help="Import log table")
@click.option('--period', '-p', required=True, type=click.STRING, help="[weekly|monthly]")
@click.option('--output_dir', '-o', default="/tmp/npi", type=click.STRING, help="Directory to store file on local filesystem")
@click.option('--limit', '-l', default=6, type=click.INT, help="Max # of files to fetch at a time.  Only weekly files are adjustable, monthly is set to 1.")
@click.option('--environment', '-e', default="dev", type=click.STRING, help="User specified environment, ie: dev|rc|stage|prod, etc")
def fetch(url_prefix, table_name, period, output_dir, limit, environment):
    """
    Fetch files from import log table
    """
    logger.info(f"Fetch '{period}' files from {table_name}")
    npi_loader = NpiLoader()
    npi_loader.connect(**{**args, **extra_args})
    npi_loader.fetch(url_prefix, table_name, period,
                     environment, output_dir, limit)


@click.command()
@click.option('--infile', '-f', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--import-table-name', '-i', default="npi_import_log", type=click.STRING, help="NPI import table")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--large-file', default=False, is_flag=True, help="Use LOAD DATA INFILE instead of INSERT")
@click.option('--initialize', default=False, is_flag=True, help="Only use for first table load to get all deactivated NPI's!  This will OVERWRITE existing data!")
@click.pass_context
def load(ctx, infile, import_table_name, table_name, period, large_file, initialize):
    """
    NPI importer
    """
    batch_size = ctx.obj['batch_size']
    throttle_size = ctx.obj['throttle_size']
    throttle_time = ctx.obj['throttle_time']
    rows = (0, 0)

    # Control output
    print_update_every = 10
    if period == "weekly":
        print_insert_every = 10
    else:
        print_insert_every = 100

    npi_loader = NpiLoader(True, print_insert_every, print_update_every)

    logger.info("NPI loader importing from {}, batch size = {} throttle size={} throttle time={}"
                .format(infile, batch_size, throttle_size, throttle_time))
    npi_loader.connect(**args)
    npi_loader.load_file(table_name, infile, batch_size,
                         throttle_size, throttle_time, initialize)

    try:
        rows = npi_loader.mark_imported(id, import_table_name)
    except Exception as e:
        logger.info(f"{e}")
        logger.info(f"Failed to update record in database.")

    npi_loader.close()
    logger.info(
        "Rows updated: {}, Rows deactivated: {}".format(rows[0], rows[1]))


@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
@click.option('--outfile', '-o', type=click.STRING, help="Filename to write out")
def npi_preprocess(infile, outfile):
    """
    Preprocess NPI csv file to do things like remove extraneous columns
    """
    npi_loader = NpiLoader()
    csv_file = npi_loader.preprocess(infile, outfile)
    logger.info(csv_file)


@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="File to unzip")
@click.option('--unzip-path', '-u', required=True, type=click.STRING, help="Directory to extract to")
def npi_unzip(infile, unzip_path):
    npi_loader = NpiLoader()
    csv_file = npi_loader.unzip(infile, unzip_path)
    logger.info(csv_file)


@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="File to unzip")
@click.option('--unzip-path', '-u', required=True, type=click.STRING, help="Directory to extract to")
@click.option('--outfile', '-o', default=None, type=click.STRING, help="Location to write cleaned file")
@click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="NPI table")
@click.option('--import-table-name', default="npi_import_log", type=click.STRING, help="NPI import table")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--workspace', '-w', default="/tmp/npi", type=click.STRING, help="Workspace directory")
@click.option('--limit', '-l', default=6, type=click.INT, help="Max # of files to fetch at a time.  Only weekly files are adjustable, monthly is set to 1.")
@click.option('--large-file', default=False, is_flag=True, help="Use LOAD DATA INFILE instead of INSERT")
@click.option('--initialize', default=False, is_flag=True, help="Only use for first table load to get all deactivated NPI's!  This will OVERWRITE existing data!")
@click.pass_context
def full_local(ctx, infile, unzip_path, outfile, batch_size, table_name, import_table_name, period, workspace, limit, large_file, initialize):
    """
    Local version of the "full" load.

    It will perform all the steps of the full load, except it does not fetch the data from s3 or update the log table metadata.  Primarily
    used to initialize the table, or for testing.
    """
    npi_loader = NpiLoader()
    logger.info(f"Unzipping {infile}")
    csv_file = npi_loader.unzip(infile, unzip_path)
    if not outfile:
        outfile = infile[:infile.rindex(".")] + ".clean.csv"
    logger.info(f"Preprocessing {csv_file}")
    npi_loader.preprocess(csv_file, outfile)
    logger.info(f"Loading cleaned file {outfile}")
    ctx.invoke(load, infile=outfile, batch_size=batch_size, table_name=table_name,
               period=period, large_file=large_file, initialize=initialize)


@click.command()
@click.option('--url-prefix', '-u', required=True, type=click.STRING, help="URL directory that contains weekly or monthly files to fetch")
@click.option('--table-name', '-t', default="npi", type=click.STRING, help="NPI table")
@click.option('--import-table-name', '-i', default="npi_import_log", type=click.STRING, help="NPI import table")
@click.option('--period', '-p', default="weekly", type=click.STRING, help="[weekly| monthly] default: weekly")
@click.option('--workspace', '-w', default="/tmp/npi", type=click.STRING, help="Workspace directory")
@click.option('--limit', '-l', default=6, type=click.INT, help="Max # of files to fetch at a time.  Only weekly files are adjustable, monthly is set to 1.")
@click.option('--environment', '-e', default="dev", type=click.STRING, help="User specified environment, ie: dev|rc|stage|prod, etc")
@click.option('--initialize', default=False, is_flag=True, help="Only use for first table load to get all deactivated NPI's!  This will OVERWRITE existing data!")
@click.pass_context
def full(ctx, url_prefix, table_name, import_table_name, period, workspace, limit, environment, initialize):
    """
    Perform a full load.  This will fetch, unzip, preprocess, and load the file from S3 into the database.  On completion, mark
    the file as imported in the log table.
    """
    logger.info(f"Loading {period} file to {table_name}")
    batch_size = ctx.obj['batch_size']
    throttle_size = ctx.obj['throttle_size']
    throttle_time = ctx.obj['throttle_time']
    rows = (0, 0)

    # Control output
    print_update_every = 10
    if period == "weekly":
        print_insert_every = 10
    else:
        print_insert_every = 100

    workspace = workspace.rstrip("/")

    # Fetch files listed in import log
    npi_fetcher = NpiLoader()
    npi_fetcher.connect(**{**args, **extra_args})
    files = npi_fetcher.fetch(
        url_prefix, import_table_name, period, environment, workspace, limit)
    npi_fetcher.close()

    # Load the files
    npi_loader = NpiLoader(True, print_insert_every, print_update_every)
    npi_loader.connect(**args)

    for infile, file_id in files.items():
        unzip_path = f"{workspace}/{infile.split('/')[-1].split('.')[0]}"
        logger.info(f"Unzipping {infile} to {unzip_path}")

        try:
            csv_file = npi_loader.unzip(infile, unzip_path)
        except Exception:
            logger.error("Error loading zip file)")
            raise

        logger.info(f"Preprocessing {csv_file}")
        try:
            cleaned_file = npi_loader.preprocess(csv_file)
        except Exception:
            logger.error("Error preprocessing file")
            raise

        try:
            logger.info("NPI loader importing from {}, batch size = {} throttle size={} throttle time={}"
                        .format(cleaned_file, batch_size, throttle_size, throttle_time))
            rows = npi_loader.load_file(table_name,
                                        cleaned_file,
                                        batch_size,
                                        throttle_size,
                                        throttle_time,
                                        initialize)
        except Exception:
            logger.error(f"Error loading data to DB")
            raise

        try:
            npi_loader.mark_imported(file_id, import_table_name)
        except Exception:
            logger.error(f"Failed to update record in database.")
            raise

    npi_loader.close()
    logger.info(
        "Rows updated: {}, Rows deactivated: {}".format(rows[0], rows[1]))


npi.add_command(load)
npi.add_command(fetch)
npi.add_command(npi_unzip, name="unzip")
npi.add_command(npi_preprocess, name="preprocess")
npi.add_command(full_local)
npi.add_command(full)
