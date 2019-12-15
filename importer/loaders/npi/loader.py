import logging
import csv, sys, subprocess, time, itertools, textwrap, datetime
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

from importer.sql.npi import (INSERT_QUERY, UPDATE_QUERY,
                              INSERT_LARGE_QUERY, GET_FILES, GET_MONTHLY_FILES, MARK_AS_IMPORTED)
from importer.sql.checks import DISABLE, ENABLE
from importer.downloaders import NpiDownloader
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def convert_date(x):
    if not str(x) == "nan":
        try:
            return datetime.datetime.strptime(str(x), '%m/%d/%Y').strftime('%Y-%m-%d')
        except Exception as e:
            pass
    return np.nan


def five_digit_zip(x):
    if pd.notnull(x):
        try:
            return x[:5]
        except Exception as e:
            pass
    return np.nan


class NpiLoader(object):
    """
    Load NPI data.  There are two loaders in this file.abs

    File loader: Loads data in batches using an INSERT query.
    Large file loader: Loads data using LOAD DATA LOCAL INFILE query.
    """

    def __init__(self, warnings=True):
        """
        Blank constructor, b/c preprocess does not need a db connection.
        """
        self.cnx = ""
        self.cursor = ""
        self.debug = ""
        self.warnings = warnings
        self.files = OrderedDict()  # OrderedDict where key is file to load, and value is id in the import log

    def connect(self, user, host, password, database, clientFlags=False, debug=False, dictionary=False, buffered=False):
        self.debug = debug

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'get_warnings': self.warnings
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

    def __nullify(self, value):
        """
        Used to convert empty strings, "", into None values so they appear as NULLs in the database.
        """
        if not str(value).strip():
            return None
        else:
            return value

    def __submit_batch(self, query, data):
        logger.debug(query)

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

        logger.debug(q.replace('\n', ' ').replace('\r', ''))
        self.cursor.execute(q)

        print(f"Fetching {self.cursor.rowcount} files")
        for row in self.cursor:
            file_name = row.get('url').split("/")[-1]

            print(f"{url_prefix}{file_name} to {output_dir}")
            dlr = NpiDownloader.get_downloader(url_prefix)
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
        path = path.strip("/")

        zip = ZipFile(infile)
        names = zip.namelist()

        # # When passed PKWare zips with Type 9 compression, we can read the zip header, but
        # #  are unable to do the extraction.  If for some reason we have a problem reading
        # #  headers, we can use the system unzip to do so, as commented below:
        # status = not subprocess.call(["which", "unzip"])
        # if status:
        #     out = subprocess.run(["unzip", "-Z1", infile], stdout=subprocess.PIPE)
        #     names = [ i.decode() for i in out.stdout.split()]

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

        try:
            zip.extract(csv_file, path)
        except NotImplementedError as e:
            print("Python does not support Type 9 compression, trying system unzip...")
            
            if not subprocess.run(["which", "unzip"]).returncode:
                out = subprocess.run(["unzip", infile, csv_file, "-d", path], stdout=subprocess.PIPE)
                if out.returncode:
                    print("Can't unzip this file.  Local unzip failed.")
                    raise
            else:
                print("Can't unzip this file.  Type 9 compression not supported, and no local unzip.")
                raise
        
        return f"{path}/{csv_file}"

    def preprocess(self, infile, outfile=None):
        """
        Given a CSV file to process (infile) and a file to write out (outfile), perform preprocessing on file.  This
        includes removing extra rows and renaming columns.  Returns the full path of the new CSV.
        """

        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"

        # # Read the header file first, then reread the entire file with just the columns we want.  This is faster
        # # than reading the entire file, and then removing the columns.
        # col_df = pd.read_csv(infile, nrows=1)
        # col_df = col_df[col_df.columns.drop(col_df.filter(regex='Healthcare Provider Taxonomy Group').columns)]
        # col_df = col_df[col_df.columns.drop(col_df.filter(regex='Provider License Number').columns)]
        # col_df = col_df[col_df.columns.drop(col_df.filter(regex='Other Provider').columns)]
        # df = pd.read_csv(infile, usecols=col_df.columns, dtype=self.__get_dtypes(), low_memory=False)
        df = pd.read_csv(infile, usecols=self.__get_columns(), dtype=self.__get_dtypes(), low_memory=False)

        # Remove type 2 data (stored as float, b/c of NaN values - pandas can't use int type for column with NaN values)
        df = df[df['Entity Type Code'] != 2.0]
        logger.debug(len(df))

        # Reformat dates to be MySQL friendly
        df['Provider Enumeration Date'] = df['Provider Enumeration Date'].apply(convert_date)
        df['Last Update Date'] = df['Last Update Date'].apply(convert_date)
        df['NPI Deactivation Date'] = df['NPI Deactivation Date'].apply(convert_date)
        df['NPI Reactivation Date'] = df['NPI Reactivation Date'].apply(convert_date)
        df['NPI Reactivation Date'] = df['NPI Reactivation Date'].apply(convert_date)

        # Only keep the first 5 digits of the zip code
        df['Provider Business Practice Location Address Postal Code'] = \
            df['Provider Business Practice Location Address Postal Code'].apply(five_digit_zip)
        
        # df = pd.read_csv(infile)
        # df = pd.read_csv(infile, low_memory=False)

        # Drop/clean columns.
        # df = df[df.columns.drop(df.filter(regex='Other Provider').columns)]
        df.columns = [self.__clean_field(col) for col in df.columns]
        
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
        query = UPDATE_QUERY.format(table_name=table_name, 
            npi_deactivation_date="%(npi_deactivation_date)s", 
            npi_reactivation_date="%(npi_reactivation_date)s", 
            last_update_date="%(last_update_date)s", 
            provider_enumeration_date="%(provider_enumeration_date)s", 
            npi="%(npi)s")
        return query

    def load_large_file(self, table_name, infile):
        """
        Load large files (such as the monthly zip).  Return the number of rows inserted.
        """
        print("NPI large loader importing from {}".format(infile))
        q = INSERT_LARGE_QUERY.format(infile=infile, table_name=table_name)

        logger.debug(repr(q))

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
            # These four date fields require some additional scrubbing once they're loaded back from
            # file, so they don't try to submit empty dates as empty strings, "".  We need them to go
            # into the database as NULL's instead.
            row['provider_enumeration_date'] = self.__nullify(row['provider_enumeration_date'])
            row['last_update_date'] = self.__nullify(row['last_update_date'])
            row['npi_deactivation_date'] = self.__nullify(row['npi_deactivation_date'])
            row['npi_reactivation_date'] = self.__nullify(row['npi_reactivation_date'])

            if row.get('npi_deactivation_date') and \
               not row.get('npi_reactivation_date') and \
               not initialize:
                # This NPI has been deactivated.  Don't blindly overwrite the old row, because we want
                # to preserve the NPI's data.  Do an UPDATE instead.
                if update_row_count >= batch_size - 1:
                    print("UPDATE batch {}".format(update_batch_count))
                    total_rows_modified += self.__submit_batch(update_q, update_batch)
                    update_batch = []
                    update_row_count = 0
                    update_batch_count += 1
                else:
                    update_row_count += 1

                # data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
                # data = OrderedDict((('npi_deactivation_date', row.get('npi_deactivation_date')), ('npi', row.get('npi'))))
                data = OrderedDict((
                    ('npi_deactivation_date', self.__nullify(row.get('npi_deactivation_date', ""))),
                    ('npi_reactivation_date', self.__nullify(row.get('npi_reactivation_date', ""))), 
                    ('last_update_date', self.__nullify(row.get('last_update_date', ""))),
                    ('provider_enumeration_date', self.__nullify(row.get('provider_enumeration_date', ""))),
                    ('npi', row.get('npi'))
                ))
                update_batch.append(data)

            else:
                if insert_row_count >= batch_size - 1:
                    print("INSERT batch {}".format(insert_batch_count))
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
            print("INSERT batch {}".format(insert_batch_count))
            total_rows_modified += self.__submit_batch(insert_q, insert_batch)

        # Submit remaining UPDATE queries
        if update_batch:
            print("UPDATE batch {}".format(update_batch_count))
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

    def __get_columns(self):
        return self.__get_dtypes().keys()

    def __get_dtypes(self):
        """
        Returns columns and column types.

        It's important to know how pandas handles int columns.  int columns with null values
        will be transformed into floats.  This results in unwanted behavior when dealing with things like telephone numbers, which are converted
        to decimal.  It can be unexpected when there is a column that normally has numeric values, but can also have numeric values.  If a weekly
        file comes in with all numeric values in this column (and some nulls), they will be treated as decimals, even though the field is normally
        treated as a string.  To get around this, we manually set the column types on these fields, rather than using the pandas type detection.

        Entity Type Code, Replacement NPI, and Provider Other Last Name Type Code are treated as floats, because these columns contain null
        values.
        """
        return {
            "NPI": np.int64,
            "Entity Type Code": np.float64,
            "Replacement NPI": np.float64,
            "Employer Identification Number (EIN)": str,
            "Provider Organization Name (Legal Business Name)": str,
            "Provider Last Name (Legal Name)": str,
            "Provider First Name": str,
            "Provider Middle Name": str,
            "Provider Name Prefix Text": str,
            "Provider Name Suffix Text": str,
            "Provider Credential Text": str,
            "Provider Other Organization Name": str,
            "Provider Other Organization Name Type Code": str,
            "Provider Other Last Name": str,
            "Provider Other First Name": str,
            "Provider Other Middle Name": str,
            "Provider Other Name Prefix Text": str,
            "Provider Other Name Suffix Text": str,
            "Provider Other Credential Text": str,
            "Provider Other Last Name Type Code": np.float64,
            "Provider First Line Business Mailing Address": str,
            "Provider Second Line Business Mailing Address": str,
            "Provider Business Mailing Address City Name": str,
            "Provider Business Mailing Address State Name": str,
            "Provider Business Mailing Address Postal Code": str,
            "Provider Business Mailing Address Country Code (If outside U.S.)": str,
            "Provider Business Mailing Address Telephone Number": str,
            "Provider Business Mailing Address Fax Number": str,
            "Provider First Line Business Practice Location Address": str,
            "Provider Second Line Business Practice Location Address": str,
            "Provider Business Practice Location Address City Name": str,
            "Provider Business Practice Location Address State Name": str,
            "Provider Business Practice Location Address Postal Code": str,
            "Provider Business Practice Location Address Country Code (If outside U.S.)": str,
            "Provider Business Practice Location Address Telephone Number": str,
            "Provider Business Practice Location Address Fax Number": str,
            "Provider Enumeration Date": str,
            "Last Update Date": str,
            "NPI Deactivation Reason Code": str,
            "NPI Deactivation Date": str,
            "NPI Reactivation Date": str,
            "Provider Gender Code": str,
            "Authorized Official Last Name": str,
            "Authorized Official First Name": str,
            "Authorized Official Middle Name": str,
            "Authorized Official Title or Position": str,
            "Authorized Official Telephone Number": str,
            "Healthcare Provider Taxonomy Code_1": str,
            "Healthcare Provider Primary Taxonomy Switch_1": str,
            "Healthcare Provider Taxonomy Code_2": str,
            "Healthcare Provider Primary Taxonomy Switch_2": str,
            "Healthcare Provider Taxonomy Code_3": str,
            "Healthcare Provider Primary Taxonomy Switch_3": str,
            "Healthcare Provider Taxonomy Code_4": str,
            "Healthcare Provider Primary Taxonomy Switch_4": str,
            "Healthcare Provider Taxonomy Code_5": str,
            "Healthcare Provider Primary Taxonomy Switch_5": str,
            "Healthcare Provider Taxonomy Code_6": str,
            "Healthcare Provider Primary Taxonomy Switch_6": str,
            "Healthcare Provider Taxonomy Code_7": str,
            "Healthcare Provider Primary Taxonomy Switch_7": str,
            "Healthcare Provider Taxonomy Code_8": str,
            "Healthcare Provider Primary Taxonomy Switch_8": str,
            "Healthcare Provider Taxonomy Code_9": str,
            "Healthcare Provider Primary Taxonomy Switch_9": str,
            "Healthcare Provider Taxonomy Code_10": str,
            "Healthcare Provider Primary Taxonomy Switch_10": str,
            "Healthcare Provider Taxonomy Code_11": str,
            "Healthcare Provider Primary Taxonomy Switch_11": str,
            "Healthcare Provider Taxonomy Code_12": str,
            "Healthcare Provider Primary Taxonomy Switch_12": str,
            "Healthcare Provider Taxonomy Code_13": str,
            "Healthcare Provider Primary Taxonomy Switch_13": str,
            "Healthcare Provider Taxonomy Code_14": str,
            "Healthcare Provider Primary Taxonomy Switch_14": str,
            "Healthcare Provider Taxonomy Code_15": str,
            "Healthcare Provider Primary Taxonomy Switch_15": str,
            "Is Sole Proprietor": str,
            "Is Organization Subpart": str,
            "Parent Organization LBN": str,
            "Parent Organization TIN": str,
            "Authorized Official Name Prefix Text": str,
            "Authorized Official Name Suffix Text": str,
            "Authorized Official Credential Text": str
        }
