import time, itertools, datetime, logging
from collections import OrderedDict
import mysql.connector as connector
from mysql.connector.constants import ClientFlag
import csv
import pandas as pd

from importer.sql import (COPY_TABLE_DDL, COPY_TABLE_DATA_DML)

logger = logging.getLogger(__name__)

def convert_date(x):
    if not str(x) == "nan":
        if not x:
            return None

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

    def __init__(self, warnings=True, batch_size=1000, throttle_size=10_000, throttle_time=3):
        self.cnx = ""
        self.cursor = ""
        self.debug = ""
        self.warnings = warnings
        self.files = OrderedDict()  # OrderedDict where key is file to load, and value is id in the import log
        self.column_type_overrides = {}  # Optional dict for functions to transform select columns {'id': int}
        self.all_columns_xform = [] # Optional list of functions to transform all columns.  Run after type overrides.
        self.batch_size = batch_size
        self.throttle_size = throttle_size
        self.throttle_time = throttle_time
        self.time = False

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

    def get_cursor(self, dictionary=False, buffered=False):
        return self.cnx.cursor(dictionary=dictionary, buffered=buffered)

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
        field_clean = field_clean.replace(";", "_")
        return field_clean.lower()

    def _nullify(self, value):
        """
        Used to convert empty strings, "", into None values so they appear as NULLs in the database.
        """
        if not str(value).strip():
            return None
        else:
            return value

    def _query(self, query, cursor=None):
        if not cursor:
            cursor = self.cursor

        if self.time:
            logger.info("Start query")
            logger.info(query)

        try:
            cursor.execute(query)
        except Exception as e:
            raise

        if self.time:
            logger.info("Query completed.")

        rows = list(cursor)
        return rows

    def _submit_single_q(self, query, commit=True):
        if self.time:
            logger.info("Start query")
            logger.info(query)

        try:
            self.cursor.execute(query)
            if commit:
                self.cnx.commit()
        except Exception as e:
            self.cnx.rollback()
            raise

        if self.time:
            logger.info("Query completed.")

        rows = list(self.cursor)
        return rows

    def _submit_batch(self, query, data):
        # logger.debug(query)
        logger.debug(f"Total data to submit: {len(data)}")

        tries = 3
        count = 0

        # Simple retry
        while count < tries:
            try:
                if self.time:
                    logger.info("Start query.")
                    logger.info(query)
                
                # cursor.execute(sql, (arg1, arg2))
                # Deadlock error here when too many processes run at once.  Implement back off timer.
                # mysql.connector.errors.InternalError: 1213 (40001): Deadlock found when trying to get lock; try restarting transaction
                self.cursor.executemany(query, data)
                self.cnx.commit()

                if self.time:
                    logger.info("Query completed.")

                if self.cursor.fetchwarnings():
                    for warning in self.cursor.fetchwarnings():
                        logger.warn(warning)
                break
            except connector.errors.InternalError as e:
                # print(self.cursor._last_executed)
                # print(self.cursor.statement)
                logger.warn("Rolling back...")
                self.cnx.rollback()
                count += 1
                logger.warn(f"Failed on try {count}/{tries}")
                if count >= tries:
                    logger.error("Could not submit batch, aborting.")
                    raise
            except connector.errors.IntegrityError as e:
                logger.warn(e)
                logger.warn("Note: this will cause entire batch to fail!")
                self.cnx.rollback()
                break
            
            count += 1

        return self.cursor.rowcount

    def _batcher(self, rows):
        """
        Return records in batches, and optionally throttle how fast records are returned.
        """
        row_count = 0
        batch = []
        batch_count = 1

        total_rows_modified = 0
        throttle_count = 0

        i = 0
        for row in rows:
            if row_count > self.batch_size - 1:
                logger.debug(f"row_count={row_count} batch_size={self.batch_size} and batch={len(batch)}")
                # Yield the previous batch
                yield batch

                # Start the new batch
                batch = []
                batch.append(row)
                row_count = 1

                batch_count += 1
                # break # toggle to load one batch only
            else:
                row_count += 1
                batch.append(row)

            # Put in a sleep timer to throttle how hard we hit the database
            if self.throttle_time and self.throttle_size and (throttle_count > self.throttle_size - 1):
                logger.info(f"Sleeping for {self.throttle_time} seconds... row: {i}")
                time.sleep(int(self.throttle_time))
                throttle_count = 0
            elif self.throttle_time and self.throttle_size:
                throttle_count += 1
            i += 1

        yield batch

    def _xform_columns(self, columns, xforms):
        """
        Perform transformations on a list of columns
        """
        new_columns = []

        for col in columns:
            if col in xforms:
                if callable(xforms[col]):
                    new_columns.append(xforms[col](col))
                else:
                    new_columns.append(xforms[col])
            else:
                new_columns.append(col)

        return new_columns

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

    def csv_loader(self, query, table_name, infile, ctx):
        """
        replacement for load_file
        """
        # unpack context
        batch_size = ctx.obj.get('batch_size', 1000)
        throttle_size = ctx.obj.get('throttle_size', 10_000)
        throttle_time = ctx.obj.get('throttle_time', 3)

        reader = csv.DictReader(open(infile, 'r'))
        # insert_q = self.build_insert_query(query, self._clean_fields(reader.fieldnames), table_name)
        logger.debug(query)
        self.row_loader(query, reader.fieldnames, reader, table_name, batch_size, throttle_size, throttle_time)

    def row_loader(self, query, columns, rows, table_name, batch_size=1000, throttle_size=10_000, throttle_time=3):
        """
        Replacement for "load_file".  This doesn't assume the file is a CSV, and instead takes a list rows, where
        each row is composed of a dictionary as follows: key is column, and value is row value.
        """
        logger.info(f"Loading data into table: {table_name}")
        clean_columns = self._clean_fields(columns)
        insert_q = self.build_insert_query(query, clean_columns, table_name)

        row_count = 0
        batch = []
        batch_count = 1

        total_rows_modified = 0
        throttle_count = 0

        i = 0
        for row in rows:
            if row_count > batch_size - 1:
                logger.info("Submitting INSERT batch {}".format(batch_count))
                total_rows_modified += self._submit_batch(insert_q, batch)
                
                logger.debug(batch)

                batch = []
                row_count = 0
                batch_count += 1

                # break # toggle to load one batch only
            else:
                row_count += 1

            data = OrderedDict()
            for key, value in row.items():
                # Perform column-specific transformations
                key = self._clean_field(key)
                if key in self.column_type_overrides:
                    try:
                        logger.debug(f"Call {self.column_type_overrides[key]} on {key}={value}")
                        value = self.column_type_overrides[key](value)
                    except Exception as e:
                        logger.debug(e)
                        logger.debug(f"Could not set value for {key}, default to None")
                        value = None
                else:
                    # If no value is defined, use null.
                    # logger.debug("key not in overrides")
                    if not value:
                        value = None

                # Perform transformation on all columns
                for xform in self.all_columns_xform:
                    try:
                        # logger.debug(f"Perform {xform} on {key} ")
                        value = xform(value)
                    except Exception as e:
                        # logger.debug(e)
                        # logger.debug(f"Could not apply {xform} to {key}={value}.  Continue...")
                        pass

                # logger.debug("\n")
                data[key] = value

            batch.append(data)

            # Put in a sleep timer to throttle how hard we hit the database
            if throttle_time and throttle_size and (throttle_count >= throttle_size - 1):
                logger.info(f"Sleeping for {throttle_time} seconds... row: {i}")
                time.sleep(int(throttle_time))
                throttle_count = 0
            elif throttle_time and throttle_size:
                throttle_count += 1
            i += 1

        # Submit remaining INSERT queries
        if batch:
            logger.debug(batch)
            logger.info("Submitting INSERT final batch {}".format(batch_count))
            total_rows_modified += self._submit_batch(insert_q, batch)
        
        return total_rows_modified



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
        logger.debug(insert_q)
        # columnNames = reader.fieldnames

        row_count = 0
        batch = []
        batch_count = 1

        total_rows_modified = 0
        throttle_count = 0

        i = 0
        for row in reader:
            if row_count >= batch_size - 1:
                logger.info("Submitting INSERT batch {}".format(batch_count))
                total_rows_modified += self._submit_batch(insert_q, batch)
                
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
                    try:
                        value = self.column_type_overrides[key](value)
                    except Exception as e:
                        logger.debug(e)
                        logger.debug(f"Could not set value for {key}, default to None")
                        value = None
                    # value = self._clean_values(key, value)
                else:
                    # If no value is defined, use null.
                    if not value:
                        value = None
                data[key] = value

            batch.append(data)

            # Put in a sleep timer to throttle how hard we hit the database
            if throttle_time and throttle_size and (throttle_count >= throttle_size - 1):
                logger.info(f"Sleeping for {throttle_time} seconds... row: {i}")
                time.sleep(int(throttle_time))
                throttle_count = 0
            elif throttle_time and throttle_size:
                throttle_count += 1
            i += 1

        # Submit remaining INSERT queries
        if batch:
            logger.debug(batch)
            logger.info("Submitting INSERT batch {}".format(batch_count))
            total_rows_modified += self._submit_batch(insert_q, batch)
        
        return total_rows_modified

    def preprocess(self, infile, outfile=None, encoding="utf-8", sep=None, column_xforms=None):
        """
        Preprocess data and write out a new file in csv format
        """
        if not outfile:
            outfile = infile[:infile.rindex(".")] + ".clean.csv"
        logger.info(f"Preprocessing {infile} and writing to {outfile}")

        if infile.endswith(".xls") or infile.endswith(".xlsx"):
            df = pd.ExcelFile(infile).parse()
        else:
            if sep:
                df = pd.read_csv(infile, encoding=encoding, sep=sep)
            else:
                df = pd.read_csv(infile, encoding=encoding)

        logger.info("Transforming and cleaning column names.")

        logger.debug(f"Columns: {df.columns}")
        if column_xforms:
            df.columns = self._xform_columns(df.columns, column_xforms)
            print("hello?")

        print("yay")
        logger.debug(f"Columns after xforms: {df.columns}")
        print("nay")

        df.columns = [ self._clean_field(col) for col in df.columns]
        logger.debug(f"Columns after cleaning: {df.columns}")

        df.to_csv(outfile, sep=',', quoting=1, index=False)
        logger.info(f"File written to {outfile}")

    def close(self):
        self.cursor.close()
        self.cnx.close()

    def execute_queries(self, queries, **format_args):
        """
        Submit multiple queries, commit transaction on final query.
        """
        query_len = len(queries)
        commit = False

        for i,query in enumerate(queries):
            q = query.format(**format_args)

            if i >= query_len - 1:
                commit = True

            self._submit_single_q(q, commit=commit)

    def copy_table(self, source_table_name, dest_table_name):
        """
        Copy a table with its data.
        """
        logger.info(f"Creating {dest_table_name} from {source_table_name}")
        try:
            self._submit_single_q(COPY_TABLE_DDL.format(new_table_name=dest_table_name, old_table_name=source_table_name))
        except connector.errors.ProgrammingError as e:
            logger.error(f"Table {dest_table_name} already exists.  Exiting.")
            return
        except Exception as e:
            logger.error("Failed creating table")
            raise

        try:
            q = COPY_TABLE_DATA_DML.format(new_table_name=dest_table_name, old_table_name=source_table_name)
            self._submit_single_q(COPY_TABLE_DATA_DML.format(new_table_name=dest_table_name, old_table_name=source_table_name))
        except Exception as e:
            logger.error("Failed inserting data")
            raise e

        # print(self._submit_single_q("checksum table xfrm_product, staging_product")

