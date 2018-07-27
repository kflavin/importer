import csv
import itertools
import textwrap
from collections import OrderedDict
import mysql.connector as connector
from mysql.connector.constants import ClientFlag
from importer.sql.npi_create_clean import CREATE_TABLE_SQL
from importer.sql.npi_insert import INSERT_WEEKLY_QUERY, INSERT_MONTHLY_QUERY
import pandas as pd

class NpiLoader(object):
    """
    Load NPI data
    """

    def __init__(self):
        """
        Blank constructor, b/c preprocess does not need a db connection.
        """
        pass

    def connect(self, user, host, password, database, clientFlags=False, debug=True):
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

        try:
            # cursor.execute(sql, (arg1, arg2))
            # Deadlock error here when too many processes run at once.  Implement back off timer.
            # mysql.connector.errors.InternalError: 1213 (40001): Deadlock found when trying to get lock; try restarting transaction
            self.cursor.executemany(query, data)
            self.cnx.commit()
        except:
            # print(self.cursor._last_executed)
            print(self.cursor.statement)
            raise
        # self.cursor.executemany(q, all_data)
        # self.cnx.commit()
    
    # def create_database(self, set_db=True):
    #     """
    #     Helper method for the class, for my testing purposes.
    #     """
    #     create_database_sql = f"create database {self.database}"
    #     self.cursor.execute(create_database_sql)
    #     self.cnx.commit()
        
    #     # Set as the active db
    #     if set_db:
    #         self.cnx.database = database

    def preprocess(self, infile, outfile):
        """
        Preprocess the CSV file before import
        """

        # Remove all the "Other Provider" columns
        # df = df[df.columns.drop(list(df.filter(regex='Test')))]
        df = pd.read_csv(infile)

        df = df[df.columns.drop(df.filter(regex='Other Provider').columns)]
        df.columns = [ self.__clean_field(col) for col in df.columns]
        
        # regex=re.compile("^other provider", re.IGNORECASE)
        # df.filter(regex='Test').columns

        df.to_csv(outfile, sep=',', quoting=1, index=False, encoding='utf-8')

    # def create_table(self, table_name):
    #     """
    #     Create the NPI table
    #     """
    #     create_table_sql = CREATE_TABLE_SQL.format(table_name=table_name)
    #     self.cursor.execute(create_table_sql)
    #     self.cnx.commit()

    def build_weekly_query(self, columns, table_name):
        """
        Construct the NPI INSERT query with all values
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
        self.cnx.commit()

    def load_weekly(self, table_name, infile, batch_size=1000):
        """
        cli loader which accepts a batch size.  This won't work in lambda for large datasets due to the
        5 min maximum timeout.
        """
        print("NPI weekly loader importing from {}, batch size = {}".format(infile, batch_size))
        reader = csv.DictReader(open(infile, 'r'))
        q = self.build_weekly_query(self.__clean_fields(reader.fieldnames), table_name)
        columnNames = reader.fieldnames

        # all_data = []
        row_count = 0
        batch = []
        batch_count = 1

        for row in reader:
            if row_count >= batch_size:
                print("Submitting batch {}".format(batch_count))
                self.__submit_batch(q, batch)
                batch = []
                row_count = 0
                batch_count += 1
            else:
                row_count += 1

            columns, values = zip(*row.items())

            # data = {key: value for key, value in row.items() }
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())

            # all_data.append(data)
            batch.append(data)

        # Get any remaining rows
        if batch:
            print("Submitting batch {}".format(batch_count))
            self.__submit_batch(q, batch)

        print("All done")