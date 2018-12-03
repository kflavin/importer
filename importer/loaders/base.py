import time, itertools, datetime, logging
from collections import OrderedDict
import mysql.connector as connector
from mysql.connector.constants import ClientFlag
import csv

logger = logging.getLogger(__name__)

def convert_date(x):
    if not str(x) == "nan":
        try:
            return datetime.datetime.strptime(str(x), '%m/%d/%Y').strftime('%Y-%m-%d')
        except Exception as e:
            return x
    else:
        return x

def convert_date_time(x):
    if not str(x) == "nan":
        try:
            return datetime.datetime.strptime(str(x), '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            return x
    else:
        return x

class BaseLoader(object):

    def __init__(self):
        self.cnx = ""
        self.cursor = ""
        self.debug = ""
        self.files = OrderedDict()  # OrderedDict where key is file to load, and value is id in the import log
        self.column_type_overrides = {}  # Optional dict to force column types, ie: {'id': int}

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

    def _clean_values(self, key, value):
        desired_column_type = self.column_type_overrides[key]

        if type(value) == str:
            if desired_column_type == int:
                try:
                    value = int(float(value))
                except:
                    pass
            elif desired_column_type == bool:
                try:
                    if value.lower() == "true":
                        value = True
                    else:
                        value = False
                except:
                    pass

        return value

    def _clean_fields(self, fields):
        columns = []

        for field in fields:
            columns.append(self._clean_field(field))

        return columns
    
    def _clean_field(self, field):
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

    def _nullify(self, value):
        """
        Used to convert empty strings, "", into None values so they appear as NULLs in the database.
        """
        if not str(value).strip():
            return None
        else:
            return value

    def _submit_batch(self, query, data):
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

    def load_file(self, query, table_name, infile, batch_size=1000, throttle_size=10_000, throttle_time=3):
        """
        Load data using the given query.

        Optionally specify a batch and throttle sizes.  Batch size controls the number of rows sent to the DB at one time.  The
        throttling will sleep throttle_time seconds for every throttle_size rows.  Throttle_size should be >= to batch_size.  If
        either throttle arg is set to 0, throttling will be disabled.
        """
        logger.info("Importing from {}, batch size = {} throttle size={} throttle time={}"\
                .format(infile, batch_size, throttle_size, throttle_time))
        reader = csv.DictReader(open(infile, 'r'))
        insert_q = self.build_insert_query(query, self._clean_fields(reader.fieldnames), table_name)
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
                total_rows_modified += self._submit_batch(insert_q, batch)
                
                logger.debug(insert_q)
                logger.debug(batch)

                batch = []
                row_count = 0
                batch_count += 1
            else:
                row_count += 1

            # data = OrderedDict((self._clean_field(key), value) for key, value in row.items())
            data = OrderedDict()
            for key, value in row.items():
                key = self._clean_field(key)
                if key in self.column_type_overrides:
                    value = self._clean_values(key, value)
                data[key] = value

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
            total_rows_modified += self._submit_batch(insert_q, batch)
        
        return total_rows_modified

    def close(self):
        self.cursor.close()
        self.cnx.close()
