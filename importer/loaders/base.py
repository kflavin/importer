import time, itertools, datetime
from collections import OrderedDict
import mysql.connector as connector
from mysql.connector.constants import ClientFlag

def convert_date(x):
    if not str(x) == "nan":
        try:
            return datetime.datetime.strptime(str(x), '%m/%d/%Y').strftime('%Y-%m-%d')
        except Exception as e:
            return None
    else:
        return None

class BaseLoader(object):

    def __init__(self):
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

    def __clean_fields(self, fields):
        columns = []

        for field in fields:
            columns.append(self.__clean_field(field))

        return columns
    
    def __clean_field(self, field):
        """
        Sanitize data
        """
        field_clean = ' '.join(field.split())   # replace multiple whitespace characters with one space
        field_clean = field_clean.replace("(", "")
        field_clean = field_clean.replace(")", "")
        field_clean = field_clean.replace(".", "")
        field_clean = field_clean.replace("", "")
        field_clean = field_clean.replace(" ", "_")
        return field_clean.lower()

    def __nullify(self, value):
        """
        Used to convert empty strings, "", into None values so they appear as NULLs in the database.
        """
        if not str(value).strip():
            return None
        else:
            return value

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

    def build_insert_query(self, query, columns, table_name):
        """
        Construct the INSERT query using a prepared statement.
        """
        cols = ""
        values = ""
        on_dupe_values = ""

        for column in columns:
            cols += "`{}`, ".format(column)
            values += "%({})s, ".format(column)
            on_dupe_values += "{} = VALUES({}), ".format(column, column)

        # Remove trailing whitespace and commas
        cols = cols.rstrip().rstrip(",")
        values = values.rstrip().rstrip(",")
        on_dupe_values = on_dupe_values.rstrip().rstrip(",")

        query = query.format(table_name=table_name, cols=cols, values=values, on_dupe_values=on_dupe_values)
        return query
