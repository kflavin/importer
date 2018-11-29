import logging, csv, sys, subprocess, time, itertools, textwrap, datetime, re
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

from importer.sql.products.product import (INSERT_QUERY)
# from importer.sql.checks import DISABLE, ENABLE
# from importer.downloaders.downloader import downloader
from importer.loaders.base import BaseLoader, convert_date

import pandas as pd

logger = logging.getLogger(__name__)

class ProductLoader(BaseLoader):
    """
    Load NPI data.  There are two loaders in this file.abs

    File loader: Loads data in batches using an INSERT query.
    Large file loader: Loads data using LOAD DATA LOCAL INFILE query.
    """

    def __init__(self):
        super(ProductLoader, self).__init__()

    # def preprocess(self, infile, outfile=None, encoding="ISO-8859-1"):
    def preprocess(self, infile, outfile=None, encoding="latin1"):
        """
        This takes an improperly formatted txt and turns it into a proper CSV :/
        """

        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        df = pd.read_csv(infile, encoding=encoding)
        df.columns = [ super(ProductLoader, self)._clean_field(col) for col in df.columns]
        df['eff_date'] = df['eff_date'].apply(convert_date)
        df['end_eff_date'] = df['end_eff_date'].apply(convert_date)
        df.to_csv(outfile, sep=',', quoting=1, index=False)


    # def load_file(self, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3):
    #     """
    #     Load NPI data using INSERT and UPDATE statements.  If a record in the data has been deactivated, then do an
    #     UPDATE instead of an INSERT to preserve the NPI data associated with the record.  This assumes the NPI record
    #     exists.  If it does not, the deactivated row will not be added.

    #     Optionally specify a batch and throttle sizes.  Batch size controls the number of rows sent to the DB at one time.  The
    #     throttling will sleep throttle_time seconds for every throttle_size rows.  Throttle_size should be >= to batch_size.  If
    #     either throttle arg is set to 0, throttling will be disabled.
    #     """
    #     logger.info("HDM loader importing from {}, batch size = {} throttle size={} throttle time={}"\
    #             .format(infile, batch_size, throttle_size, throttle_time))
    #     reader = csv.DictReader(open(infile, 'r'))
    #     insert_q = super().build_insert_query(INSERT_QUERY, super()._clean_fields(reader.fieldnames), table_name)
    #     # columnNames = reader.fieldnames

    #     row_count = 0
    #     batch = []
    #     batch_count = 1

    #     total_rows_modified = 0
    #     throttle_count = 0

    #     i = 0
    #     for row in reader:
    #         if row_count >= batch_size - 1:
    #             print("Submitting INSERT batch {}".format(batch_count))
    #             total_rows_modified += super()._submit_batch(insert_q, batch)
    #             batch = []
    #             row_count = 0
    #             batch_count += 1
    #         else:
    #             row_count += 1

    #         data = OrderedDict((super(ProductLoader, self)._clean_field(key), value) for key, value in row.items())
    #         batch.append(data)

    #         # Put in a sleep timer to throttle how hard we hit the database
    #         if throttle_time and throttle_size and (throttle_count >= throttle_size - 1):
    #             print(f"Sleeping for {throttle_time} seconds... row: {i}")
    #             time.sleep(int(throttle_time))
    #             throttle_count = 0
    #         elif throttle_time and throttle_size:
    #             throttle_count += 1
    #         i += 1

    #     # Submit remaining INSERT queries
    #     if batch:
    #         print("Submitting INSERT batch {}".format(batch_count))
    #         total_rows_modified += super()._submit_batch(insert_q, batch)

    #     return total_rows_modified
