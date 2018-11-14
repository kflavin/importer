import logging, csv, sys, subprocess, time, itertools, textwrap, datetime
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

from importer.sql.hdm import (INSERT_QUERY)
# from importer.sql.checks import DISABLE, ENABLE
# from importer.downloaders.downloader import downloader
from importer.loaders.base import BaseLoader, convert_date

import pandas as pd

logger = logging.getLogger(__name__)

class HdmLoader(BaseLoader):
    """
    Load NPI data.  There are two loaders in this file.abs

    File loader: Loads data in batches using an INSERT query.
    Large file loader: Loads data using LOAD DATA LOCAL INFILE query.
    """

    def __init__(self):
        super(HdmLoader, self).__init__()

    def fetch(self, url_prefix, table_name, period, environment, output_dir, limit):
        """
        Given a URL prefix (containing a directory of files), search the NPI import log for files
        ready for import.  Fetch them into the output directory.

        url_prefix: A directory containing files, ie: s3://mybucket/my/prefix/
        table_name: Name of the NPI import log table
        period: [weekly|monthly]
        environment: User specified environment, ie: dev|rc|stage|prod, etc
        output_dir: Directory to save the file locally
        limit: max # of files to load.  The monthly files are hardcoded to 1.
        """
        url_prefix = url_prefix.rstrip("/") + "/"

        if period.lower() == "weekly":
            p = "w"
            limit = limit
            q = GET_FILES.format(table_name=table_name, period=p, environment=environment, limit=limit)
        else:
            p = "m"
            # We only want to load one monthly file at a time.  Pick the most recent one.
            q = GET_MONTHLY_FILES.format(table_name=table_name, period=p, environment=environment)

        print(q.replace('\n', ' ').replace('\r', ''))
        self.cursor.execute(q)

        print(f"Fetching {self.cursor.rowcount} files")
        for row in self.cursor:
            file_name = row.get('url').split("/")[-1]

            print(f"{url_prefix}{file_name} to {output_dir}")
            dlr = downloader(url_prefix)
            downloaded_file = dlr.download(file_name, output_dir)

            if downloaded_file:
                self.files[downloaded_file] = row['id']

        files_reversed = OrderedDict(reversed(list(self.files.items())))   # we receive in desc order, so reverse for processing
        print(f"Files to process: {files_reversed}")
        return files_reversed

    def preprocess(self, infile, outfile=None):
        """
        Given a CSV file to process (infile) and a file to write out (outfile), perform preprocessing on file.  This
        includes removing extra rows and renaming columns.  Returns the full path of the new CSV.
        """

        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        df = pd.ExcelFile(infile).parse()

        # col_df = col_df[col_df.columns.drop(col_df.filter(regex='Healthcare Provider Taxonomy Group').columns)]
        # col_df = col_df[col_df.columns.drop(col_df.filter(regex='Provider License Number').columns)]
        # col_df = col_df[col_df.columns.drop(col_df.filter(regex='Other Provider').columns)]
        # df = pd.read_csv(infile, usecols=col_df.columns, low_memory=False)
        
        # Remove type 2 data (stored as float, b/c of NaN values - pandas can't use int type for column with NaN values)
        # df = df[df['Entity Type Code'] != 2.0]

        # Reformat dates to be MySQL friendly
        # df['Provider Enumeration Date'] = df['Provider Enumeration Date'].apply(convert_date)
        # df['Last Update Date'] = df['Last Update Date'].apply(convert_date)
        # df['NPI Deactivation Date'] = df['NPI Deactivation Date'].apply(convert_date)
        # df['NPI Reactivation Date'] = df['NPI Reactivation Date'].apply(convert_date)
        
        # df = pd.read_csv(infile)
        # df = pd.read_csv(infile, low_memory=False)

        # Drop/clean columns.
        # df = df[df.columns.drop(df.filter(regex='Other Provider').columns)]
        df.columns = [ super()._clean_field(col) for col in df.columns]
        df['eff_date'] = df['eff_date'].apply(convert_date)
        df['end_eff_date'] = df['end_eff_date'].apply(convert_date)
        
        df.to_csv(outfile, sep=',', quoting=1, index=False, encoding='utf-8')

        return outfile

    def load_file(self, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3):
        """
        Load NPI data using INSERT and UPDATE statements.  If a record in the data has been deactivated, then do an
        UPDATE instead of an INSERT to preserve the NPI data associated with the record.  This assumes the NPI record
        exists.  If it does not, the deactivated row will not be added.

        Optionally specify a batch and throttle sizes.  Batch size controls the number of rows sent to the DB at one time.  The
        throttling will sleep throttle_time seconds for every throttle_size rows.  Throttle_size should be >= to batch_size.  If
        either throttle arg is set to 0, throttling will be disabled.
        """
        logger.info("HDM loader importing from {}, batch size = {} throttle size={} throttle time={}"\
                .format(infile, batch_size, throttle_size, throttle_time))
        reader = csv.DictReader(open(infile, 'r'))
        insert_q = super().build_insert_query(INSERT_QUERY, super()._clean_fields(reader.fieldnames), table_name)
        # columnNames = reader.fieldnames

        row_count = 0
        batch = []
        batch_count = 1

        total_rows_modified = 0
        throttle_count = 0

        i = 0
        for row in reader:
            if row_count >= batch_size - 1:
                print("Submitting INSERT batch {}".format(batch_count))
                total_rows_modified += super()._submit_batch(insert_q, batch)
                batch = []
                row_count = 0
                batch_count += 1
            else:
                row_count += 1

            data = OrderedDict((super(HdmLoader, self)._clean_field(key), value) for key, value in row.items())
            batch.append(data)

            # Put in a sleep timer to throttle how hard we hit the database
            if throttle_time and throttle_size and (throttle_count >= throttle_size - 1):
                print(f"Sleeping for {throttle_time} seconds... row: {i}")
                time.sleep(int(throttle_time))
                throttle_count = 0
            elif throttle_time and throttle_size:
                throttle_count += 1
            i += 1

        # Submit remaining INSERT queries
        if batch:
            print("Submitting INSERT batch {}".format(batch_count))
            total_rows_modified += super()._submit_batch(insert_q, batch)

        return total_rows_modified

    # def disable_checks(self):
    #     """
    #     Temporarily disable DB checks while loading large data sets
    #     """
    #     self.cursor.execute(DISABLE, multi=True)

    # def enable_checks(self):
    #     self.cursor.execute(ENABLE, multi=True)

    def close(self):
        self.cursor.close()
        self.cnx.close()

    # def mark_imported(self, id, table_name):
    #     """
    #     Mark a file as imported in the db
    #     """
    #     query = MARK_AS_IMPORTED.format(table_name=table_name, id=id)
    #     self.cursor.execute(query)
    #     self.cnx.commit()
