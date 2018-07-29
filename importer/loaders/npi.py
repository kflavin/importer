import csv
import sys
import itertools
import textwrap
from collections import OrderedDict
from zipfile import ZipFile
import mysql.connector as connector
from mysql.connector.constants import ClientFlag
from importer.sql.npi_create_clean import CREATE_TABLE_SQL
from importer.sql.npi_insert import INSERT_WEEKLY_QUERY, INSERT_MONTHLY_QUERY
import pandas as pd

class NpiLoader(object):
    """
    Load NPI data.  There are two loaders in this file.abs

    Weekly Loader: Loads data in batches using an INSERT query.  Files are presumed to be smaller.
    Monthly Loader: Loads data using LOAD DATA LOCAL INFILE query.  Files are presumed to be larger.
    """

    def __init__(self):
        """
        Blank constructor, b/c preprocess does not need a db connection.
        """
        pass

    def connect(self, user, host, password, database, clientFlags=False, debug=False):
        self.debug = debug

        config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database
        }

        if clientFlags:
            config['client_flags'] = [ClientFlag.LOCAL_FILES]

        self.cnx = connector.connect(**config)
        self.cursor = self.cnx.cursor()

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
        return field_clean

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
            except mysql.connector.errors.InternalError as e:
                # print(self.cursor._last_executed)
                # print(self.cursor.statement)
                print("Rolling back...")
                self.cnx.rollback()
                count += 1
                print("Failed on try {count}/{tries}")
                if count < tries:
                    print("Could not submit batch")
                    raise

        return self.cursor.rowcount

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


    def preprocess(self, infile, outfile):
        """
        Given a CSV file to process (infile) and a file to write out (outfile), perform preprocessing on file.  This
        includes removing extra rows and renaming columns.  Returns the full path of the new CSV.
        """

        # Remove all the "Other Provider" columns
        # df = df[df.columns.drop(list(df.filter(regex='Test')))]
        df = pd.read_csv(infile, low_memory=False)

        df = df[df.columns.drop(df.filter(regex='Other Provider').columns)]
        df.columns = [ self.__clean_field(col) for col in df.columns]
        
        # regex=re.compile("^other provider", re.IGNORECASE)
        # df.filter(regex='Test').columns

        df.to_csv(outfile, sep=',', quoting=1, index=False, encoding='utf-8')

    def build_weekly_query(self, columns, table_name):
        """
        Construct the NPI INSERT query to use with the weekly loader.
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

        query = INSERT_WEEKLY_QUERY.format(table_name=table_name, cols=cols, values=values, on_dupe_values=on_dupe_values)
        return query

    def load_monthly(self, table_name, infile):
        """
        Load monthly data (larger) file
        """
        print("NPI monthly loader importing from {}".format(infile))
        q = INSERT_MONTHLY_QUERY.format(infile=infile, table_name=table_name)
        
        if self.debug:
            print(repr(q))

        self.cursor.execute(q)
        print(f"{self.cursor.rowcount} rows inserted")
        self.cnx.commit()


    def load_weekly(self, table_name, infile, batch_size=1000):
        """
        Load weekly data (smaller) file.  Size of INSERT can be broken up into batches (batch_size)
        """
        print("NPI weekly loader importing from {}, batch size = {}".format(infile, batch_size))
        reader = csv.DictReader(open(infile, 'r'))
        q = self.build_weekly_query(self.__clean_fields(reader.fieldnames), table_name)
        columnNames = reader.fieldnames

        row_count = 0
        batch = []
        batch_count = 1
        total_rows_inserted = 0

        for row in reader:
            if row_count >= batch_size:
                print("Submitting batch {}".format(batch_count))
                total_rows_inserted += self.__submit_batch(q, batch)
                batch = []
                row_count = 0
                batch_count += 1
            else:
                row_count += 1

            columns, values = zip(*row.items())

            # data = {key: value for key, value in row.items() }
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())

            batch.append(data)

        # Get any remaining rows
        if batch:
            print("Submitting batch {}".format(batch_count))
            total_rows_inserted += self.__submit_batch(q, batch)

        print(f"{total_rows_inserted} rows inserted")