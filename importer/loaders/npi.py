import csv
import sys
import time
import itertools
import textwrap
import datetime
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

from importer.sql.npi import (CREATE_NPI_TABLE, INSERT_QUERY, UPDATE_QUERY,
                              INSERT_LARGE_QUERY, GET_FILES, GET_MONTHLY_FILES, MARK_AS_IMPORTED)
from importer.sql.checks import DISABLE, ENABLE
from importer.downloaders.downloader import downloader
import pandas as pd

def convert_date(x):
    if not str(x) == "nan":
        return datetime.datetime.strptime(str(x), '%m/%d/%Y').strftime('%Y-%m-%d')
    else:
        return None

class NpiLoader(object):
    """
    Load NPI data.  There are two loaders in this file.abs

    File loader: Loads data in batches using an INSERT query.
    Large file loader: Loads data using LOAD DATA LOCAL INFILE query.
    """

    def __init__(self):
        """
        Blank constructor, b/c preprocess does not need a db connection.
        """
        self.cnx = ""
        self.cursor = ""
        self.debug = ""
        self.files = OrderedDict()  # OrderedDict where key is file to load, and value is id in the import log

    def connect(self, user, host, password, database, clientFlags=False, debug=False, dictionary=False, buffered=False):
        self.debug = debug

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database
        }

        # Needed for LOAD DATA INFILE LOCAL
        if clientFlags:
            config['client_flags'] = [ClientFlag.LOCAL_FILES]

        self.cnx = connector.connect(**config)
        self.cursor = self.cnx.cursor(dictionary=dictionary, buffered=buffered)

    def __clean_field(self, field):
        """
        Sanitize NPI data
        """
        field_clean = ' '.join(field.split())   # replace multiple whitespace characters with one space
        field_clean = field_clean.replace("(", "")
        field_clean = field_clean.replace(")", "")
        field_clean = field_clean.replace(".", "")
        field_clean = field_clean.replace("", "")
        field_clean = field_clean.replace(" If outside US", "")
        field_clean = field_clean.replace(" ", "_")
        return field_clean.lower()

    def __clean_fields(self, fields):
        columns = []

        for field in fields:
            columns.append(self.__clean_field(field))

        return columns

    def __submit_batch(self, query, data):
        if self.debug:
            print(query)

        tries = 3
        count = 0

        # Simple retry
        while count < tries:
            try:
                # cursor.execute(sql, (arg1, arg2))
                # Deadlock error here when too many processes run at once.  Implement back off timer.
                # mysql.connector.errors.InternalError: 1213 (40001): Deadlock found when trying to get lock; try restarting transaction
                self.cursor.executemany(query, data)
                self.cnx.commit()
                break
            except connector.errors.InternalError as e:
                # print(self.cursor._last_executed)
                # print(self.cursor.statement)
                print("Rolling back...")
                self.cnx.rollback()
                count += 1
                print(f"Failed on try {count}/{tries}")
                if count >= tries:
                    print("Could not submit batch, aborting.")
                    raise

        return self.cursor.rowcount

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

    def unzip(self, infile, path):
        """
        Given a zip file (infile) and path to unzip to (path), unzip the file and return the full path.
        """
        zip = ZipFile(infile)
        names = zip.namelist()

        extractions = []
        for name in names:
            if name.startswith("npidata_pfile_") and \
               name.endswith(".csv") and \
               not "FileHeader" in name:
                extractions.append(name)

        if len(extractions) != 1:
            print("Did not find exactly one file in {infile}.  Exiting.")
            sys.exit(1)

        csv_file = extractions[0]
        zip.extract(csv_file, path)
        return f"{path}/{csv_file}"

    def preprocess(self, infile, outfile=None):
        """
        Given a CSV file to process (infile) and a file to write out (outfile), perform preprocessing on file.  This
        includes removing extra rows and renaming columns.  Returns the full path of the new CSV.
        """

        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        # Read the header file first, then reread the entire file with just the columns we want.  This is faster
        # than reading the entire file, and then removing the columns.
        col_df = pd.read_csv(infile, nrows=1)
        col_df = col_df[col_df.columns.drop(col_df.filter(regex='Healthcare Provider Taxonomy Group').columns)]
        col_df = col_df[col_df.columns.drop(col_df.filter(regex='Provider License Number').columns)]
        col_df = col_df[col_df.columns.drop(col_df.filter(regex='Other Provider').columns)]
        df = pd.read_csv(infile, usecols=col_df.columns, low_memory=False)
        
        # Remove type 2 data (stored as float, b/c of NaN values - pandas can't use int type for column with NaN values)
        df = df[df['Entity Type Code'] != 2.0]

        # Reformat dates to be MySQL friendly
        df['Last Update Date'] = df['Last Update Date'].apply(convert_date)
        df['NPI Deactivation Date'] = df['NPI Deactivation Date'].apply(convert_date)
        df['NPI Reactivation Date'] = df['NPI Reactivation Date'].apply(convert_date)
        
        # df = pd.read_csv(infile)
        # df = pd.read_csv(infile, low_memory=False)

        # Drop/clean columns.
        # df = df[df.columns.drop(df.filter(regex='Other Provider').columns)]
        df.columns = [ self.__clean_field(col) for col in df.columns]
        
        # regex=re.compile("^other provider", re.IGNORECASE)
        # df.filter(regex='Test').columns

        df.to_csv(outfile, sep=',', quoting=1, index=False, encoding='utf-8')

        return outfile

    def build_insert_query(self, columns, table_name):
        """
        Construct the NPI INSERT query.
        """
        cols = ""
        values = ""
        on_dupe_values = ""

        for column in columns:
            cols += "`{}`, ".format(column)
            values += "%({})s, ".format(column)
            on_dupe_values += "{} = VALUES({}), ".format(column, column)

        cols = cols.rstrip().rstrip(",")
        values = values.rstrip().rstrip(",")
        on_dupe_values = on_dupe_values.rstrip().rstrip(",")

        query = INSERT_QUERY.format(table_name=table_name, cols=cols, values=values, on_dupe_values=on_dupe_values)
        return query

    def build_update_query(self, table_name):
        """
        Construct the NPI UPDATE query.  Used to preserve data when an NPI has been deactivated.
        """
        query = UPDATE_QUERY.format(table_name=table_name, npi_deactivation_date="%(npi_deactivation_date)s", npi="%(npi)s")
        return query

    def load_large_file(self, table_name, infile):
        """
        Load large files (such as the monthly zip).  Return the number of rows inserted.
        """
        print("NPI large loader importing from {}".format(infile))
        q = INSERT_LARGE_QUERY.format(infile=infile, table_name=table_name)
        
        if self.debug:
            print(repr(q))

        self.cursor.execute(q)
        self.cnx.commit()
        return self.cursor.rowcount

    def load_file(self, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3, initialize=False):
        """
        Load NPI data using INSERT and UPDATE statements.  If a record in the data has been deactivated, then do an
        UPDATE instead of an INSERT to preserve the NPI data associated with the record.  This assumes the NPI record
        exists.  If it does not, the deactivated row will not be added.

        Optionally specify a batch and throttle sizes.  Batch size controls the number of rows sent to the DB at one time.  The
        throttling will sleep throttle_time seconds for every throttle_size rows.  Throttle_size should be >= to batch_size.  If
        either throttle arg is set to 0, throttling will be disabled.
        """
        print("NPI loader importing from {}, batch size = {} throttle size={} throttle time={}"\
                .format(infile, batch_size, throttle_size, throttle_time))
        reader = csv.DictReader(open(infile, 'r'))
        insert_q = self.build_insert_query(self.__clean_fields(reader.fieldnames), table_name)
        update_q = self.build_update_query(table_name)
        columnNames = reader.fieldnames

        # Maintain two batches.  One for INSERT queries, and one for UPDATE queries.
        insert_row_count = 0
        insert_batch = []
        insert_batch_count = 1

        update_row_count = 0
        update_batch = []
        update_batch_count = 1

        total_rows_modified = 0
        throttle_count = 0

        i = 0
        for row in reader:
            if row.get('npi_deactivation_date') and \
               not row.get('npi_reactivation_date') and \
               not initialize:
                # This NPI has been deactivated.  Don't blindly overwrite the old row, because we want
                # to preserve the NPI's data.  Do an UPDATE instead.
                if update_row_count >= batch_size - 1:
                    print("Submitting UPDATE batch {}".format(update_batch_count))
                    total_rows_modified += self.__submit_batch(update_q, update_batch)
                    update_batch = []
                    update_row_count = 0
                    update_batch_count += 1
                else:
                    update_row_count += 1

                # data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
                data = OrderedDict((('npi_deactivation_date', row.get('npi_deactivation_date')), ('npi', row.get('npi'))))
                update_batch.append(data)

            else:
                if insert_row_count >= batch_size - 1:
                    print("Submitting INSERT batch {}".format(insert_batch_count))
                    total_rows_modified += self.__submit_batch(insert_q, insert_batch)
                    insert_batch = []
                    insert_row_count = 0
                    insert_batch_count += 1
                else:
                    insert_row_count += 1

                data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
                insert_batch.append(data)

            # Put in a sleep timer to throttle how hard we hit the database
            if throttle_time and throttle_size and (throttle_count >= throttle_size - 1):
                print(f"Sleeping for {throttle_time} seconds... row: {i}")
                time.sleep(int(throttle_time))
                throttle_count = 0
            elif throttle_time and throttle_size:
                throttle_count += 1
            i += 1

        # Submit remaining INSERT queries
        if insert_batch:
            print("Submitting INSERT batch {}".format(insert_batch_count))
            total_rows_modified += self.__submit_batch(insert_q, insert_batch)

        # Submit remaining UPDATE queries
        if update_batch:
            print("Submitting UPDATE batch {}".format(update_batch_count))
            print(len(update_batch))
            total_rows_modified += self.__submit_batch(update_q, update_batch)

        return total_rows_modified

    def disable_checks(self):
        """
        Temporarily disable DB checks while loading large data sets
        """
        self.cursor.execute(DISABLE, multi=True)

    def enable_checks(self):
        self.cursor.execute(ENABLE, multi=True)

    def close(self):
        self.cursor.close()
        self.cnx.close()

    def mark_imported(self, id, table_name):
        """
        Mark a file as imported in the db
        """
        query = MARK_AS_IMPORTED.format(table_name=table_name, id=id)
        self.cursor.execute(query)
        self.cnx.commit()
