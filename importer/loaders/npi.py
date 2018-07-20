import csv
import itertools
import textwrap
from collections import OrderedDict
import mysql.connector as connector
from importer.sql.npi_create_clean import CREATE_TABLE_SQL
import pandas as pd

class NpiLoader(object):
    """
    Load NPI data
    """

    def __init__(self):
        pass

    def connect(self, user, host, password, database=None):
        self.cnx = connector.connect(user=user, password=password, host=host)

        # When creating the database for the first time, set to False
        if database:
            self.database = database
            self.cnx.database = database

        self.cursor = self.cnx.cursor()

    def set_db(self, database):
        self.database = database

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

    def __clean_fields(self, fields):
        columns = []

        for field in fields:
            columns.append(self.__clean_field(field))

        return columns
    
    def create_database(self, set_db=True):
        """
        Helper method for the class, for my testing purposes.
        """
        create_database_sql = f"create database {self.database}"
        self.cursor.execute(create_database_sql)
        self.cnx.commit()
        
        # Set as the active db
        if set_db:
            self.cnx.database = database

    def create_table(self, table_name):
        """
        Create the NPI table
        """
        create_table_sql = CREATE_TABLE_SQL.format(table_name=table_name)
        self.cursor.execute(create_table_sql)
        self.cnx.commit()


    def insert_query(self, columns, table_name):
        """
        Construct the NPI INSERT query
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


        query = f"""
            INSERT INTO {table_name}
            ({cols})
            VALUES ({values})
            ON DUPLICATE KEY UPDATE
            {on_dupe_values}
        """

        print(textwrap.dedent(query))

        return query

    def __submit_batch(self, query, data):
        # print("Execute query")
        try:
            # cursor.execute(sql, (arg1, arg2))
            self.cursor.executemany(query, data)
            # print("Commit query")
            self.cnx.commit()
        except:
            # print(self.cursor._last_executed)
            print(self.cursor.statement)
            raise
        # self.cursor.executemany(q, all_data)
        # self.cnx.commit()

    def step_load(self, table_name, infile, start, end=None):
        """
        For use with AWS step functions.  If your CSV has headers, start=0 will be your header.  It's
        up to the user to skip this row when using this function.
        """
        print("NPI loader for step functions.  Start = {}, End = {}".format(start, end))

        # Get the file headers first
        with open(infile, 'r') as headerFile:
            columnNames = csv.DictReader(headerFile).fieldnames

        fileh = open(infile, 'r')
        lines = [line for line in itertools.islice(fileh, start, end)]
        reader = csv.DictReader(lines, fieldnames=columnNames)

        # Our INSERT query
        q = self.insert_query(self.__clean_fields(columnNames), table_name)

        batch = []
        for row in reader:
            columns, values = zip(*row.items())
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
            batch.append(data)

        self.__submit_batch(q, batch)



    def step_load2(self, table_name, infile, position, batch_size):
        """
        This attempts to use file byte locations to split the file.  It is not working, because tell()
        can't be used in conjunction with the CSV module, which call next().
        """
        print("NPI loader2 for step functions.  Batch size = {}".format(batch_size))

        # Get the file headers first
        with open(infile, 'r') as headerFile:
            columnNames = csv.DictReader(headerFile).fieldnames

        # Our INSERT query
        q = self.insert_query(self.__clean_fields(columnNames), table_name)

        # Now proceed with the data
        fileh = open(infile, 'r')
        fileh.seek(position)
        
        reader = csv.DictReader(fileh, fieldnames=columnNames)

        batch = []
        for i, row in enumerate(reader):
            if not i < batch_size:
                break

            columns, values = zip(*row.items())
            data = OrderedDict((self.__clean_field(key), value) for key, value in row.items())
            batch.append(data)

        self.__submit_batch(q, batch)

        end_pos = fileh.tell()
        print("End position is {}".format(end_pos))
        return end_pos

    def step_load3(self, table_name, infile, position=0, batch_size=1000):
        """
        Use byte locations, and read line by line
        """
        print("NPI loader3 for step functions.  Batch size = {}".format(batch_size))

        fileh = open(infile, 'r')
        fileh.seek(position)

        batch = []
        count = 0
        for line in fileh:
            csv_line = pp.commaSeparatedList.copy().addParseAction(pp.tokenMap(lambda s: s.strip('"')))
            batch.append(csv_line)

        print(batch)


        print(f"File position is {fileh.tell()}")


        # print(csv_line.parseString(cStr).asList())


    def load(self, table_name, infile, batch_size=1000):
        """
        cli loader which accepts a batch size.  This won't work in lambda for large datasets due to the
        5 min maximum timeout.
        """
        print("NPI loader, batch size = {}".format(batch_size))
        # reader = csv.DictReader(open(infile, 'r'))
        reader = csv.DictReader(infile)
        # headers = [ key for key in next(reader).keys() ]
        # q = self.insert_query(headers)
        q = self.insert_query(self.__clean_fields(reader.fieldnames), table_name)
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