import click
import os
import logging
import sys
import re
from collections import OrderedDict
from numpy import dtype
import pandas as pd
from pandas.api.types import (is_string_dtype, is_int64_dtype, is_integer,
                            is_numeric_dtype, is_float_dtype)

from importer.loaders.base import BaseLoader
# from importer.loaders.products.base import DeltaBaseLoader
from importer.sql import INSERT_QUERY, COLUMN_ENCODING_DML

# print(logging.Logger.manager.loggerDict)

def get_int_type(val):
    if val < 2147483647:
        return "INT"
    else:
        return "BIGINT"

def get_float_type(val):
    left = str(val).find(".")
    right = str(123.332)[::-1].find(".")
    digit_count = left + right
    return f"DECIMAL({digit_count}, {right})"

def create_table_sql(ordered_columns, table_name):
    """
    Output SQL for creating the table, include an ID primary key.
    """
    print(f"CREATE TABLE IF NOT EXISTS `{table_name}` (")
    print("  `id` INT NOT NULL AUTO_INCREMENT,")
    for col,d in ordered_columns.items():
        print(f"  `{col}` {d['type']} DEFAULT NULL,")
    print("   PRIMARY KEY (`id`)")
    print(") ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;")
    print("-- Be sure to verify the column values!")

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def tools(ctx):
    ctx.ensure_object(dict)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--table-name', '-t', required=True, type=click.STRING, help="Table name to load.")
@click.pass_context
def load(ctx, infile, table_name):
    """
     Generic loader
    """
    batch_size = ctx.obj['batch_size']
    throttle_size = ctx.obj['throttle_size']
    throttle_time = ctx.obj['throttle_time']
    debug = ctx.obj['debug']

    args = {
        'user': os.environ.get('loader_db_user'),
        'password': os.environ.get('loader_db_password'),
        'host': os.environ.get('loader_db_host'),
        'database': os.environ.get('loader_db_name'),
        'debug': debug
    }

    logger.debug("Loading: query={} table={} infile={} batch_size={} throttle_size={} throttle_time={} \n".format(
        INSERT_QUERY, table_name, infile, batch_size, throttle_size, throttle_time
    ))

    loader = BaseLoader()
    # loader.column_type_overrides = {
    #     'rx': (lambda x: True if x.lower() == "true" else False),
    #     'otc': (lambda x: True if x.lower() == "true" else False),
    #     'phoneextension': (lambda x: float(int(x)) if x else None),
    #     'containsdinumber': (lambda x: float(int(x)) if x else None)
    #     # 'pkgquantity': (lambda x: float(int(x)) if x else None)
    # }
    loader.warnings = True
    logger.info(f"Loading {infile} into {table_name}")
    loader.connect(**args)
    loader.load_file(INSERT_QUERY, table_name, infile, batch_size, throttle_size, throttle_time)

    print(f"Data loaded to table: {table_name}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="Excel file with Product Master data")
@click.option('--outfile', '-o', type=click.STRING, help="CSV filename to write out")
@click.option('--separator', '-s', type=click.STRING, help="CSV separator (optional)")
@click.option('--encoding', '-e', default='utf-8', type=click.STRING, help="CSV encoding, default 'utf-8'")
@click.option('--drop-columns', '-d', type=click.STRING, help="")
def preprocess(infile, outfile, separator, encoding, drop_columns):
    """
    Preprocess device files
    """
    loader = BaseLoader()
    loader.preprocess(infile, outfile, encoding=encoding, sep=separator, drop_columns=drop_columns)

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with table data")
@click.option('--encoding', '-e', default="utf-8", type=click.STRING, help="Character encoding.  Default 'utf-8'")
@click.option('--show-errors/--no-show-errors', default=True, help="Display problematic lines.")
@click.option('--out-encoding', '-e', default="latin1", type=click.STRING, help="Encoding used to print out problematic characters")
@click.pass_context
def invalid_chars(ctx, infile, encoding, show_errors, out_encoding):
    """
    Detect invalid character in a CSV file.
    """
    with open(infile, mode='r', encoding=encoding) as f:

        line = "start"
        pattern = re.compile(".*can't decode byte (0x[a-fA-F0-9]+)")
        s = set()

        lineno = 1
        count = 0
        while line:
            try:
                line = f.readline()
            except UnicodeDecodeError as e:
                print(f"line {str(lineno):10}: {str(e)}")
                match = pattern.match(str(e))
                if match:
                    s.add(match.groups()[0])
                count += 1
            lineno += 1


    # This will only show the first bad character in a line
    if show_errors:
        with open(infile, mode='r', encoding=out_encoding) as f:
            for line in f:
                for i in s:
                    # print(f"\{i[1:]}")
                    search = chr(int(i, 16))
                    loc = line.find(search)
                    if loc != -1:
                        print(f"{line.strip()}")
                        print("{:>{loc}}".format("^", loc=loc+1))

    print("\nInvalid characters detected:")
    print(s)
    for i in s:
        print(i + ": " + chr(int(i, 16)))
    print(f"Total bad lines: {count}")

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with table data")
@click.pass_context
def x2c(ctx, infile):
    """
    Convert Excel file to CSV.  Does NOT handle multiple worksheets!
    """
    df = pd.ExcelFile(infile).parse()
    outfile = ".".join(infile.split(".")[:-1]) + ".csv"
    df.to_csv(outfile, sep=',', quoting=1, index=False, encoding='utf-8')

@click.command()
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with table data")
@click.option('--table-name', '-t', default="[TABLE_NAME]", type=click.STRING, help="Table name.")
@click.option('--col-spacing', '-s', default=20, type=click.INT, help="Spacing between columns.")
@click.option('--varchar-factor', default=1, type=click.INT, help="Factor for creating varchar field.  Defaults to 2x max value length.")
@click.option('--sql/--no-sql', default=True, help="Display create table SQL.")
@click.option('--encoding', '-e', default="utf-8", type=click.STRING, help="Character encoding.  Default 'utf-8'")
@click.option('--separator', '-s', default=",", type=click.STRING, help="CSV separator (optional)")
@click.pass_context
def create_table(ctx, infile, table_name, col_spacing, varchar_factor, sql, encoding, separator):
    """
    Display SQL table create command from a CSV file.
    """

    ordered_columns = OrderedDict()

    if infile.endswith(".xls") or infile.endswith(".xlsx"):
        print("Loading Excel file...")
        df = pd.ExcelFile(infile).parse()

        # # Use nrows to limit the number of rows read
        # workbook = pd.ExcelFile(workbook_filename)
        # rows = workbook.book.sheet_by_index(0).nrows
        # nrows = 10
        # workbook_dataframe = pd.read_excel(workbook, nrows=#rows to parse)
    else:
        print("Loading CSV file...")
        df = pd.read_csv(infile, encoding=encoding, sep=separator)

    count = 0
    for column in df.columns:
        #print(df[column].dtype)
        count += 1
        sys.stdout.write(f"{str(count):3} ")

        # The entire column is empty.  No rows have values.
        if df[column].isna().all():
            ordered_columns[column] = {'type': None, 'length': None}
            print("{:{col_spacing}}: {}".format("No values", column, col_spacing=col_spacing))
            continue

        # Handling of numeric fields
        if is_numeric_dtype(df[column]):
            # Find the max value
            maxVal = None
            validVals = [i for i in df[column].dropna()]
            if validVals:
                maxVal = max(validVals)

            if is_float_dtype(df[column]):
                # Pandas stores numerical columns with null values as floats.  We
                # need to do some extra work to determine if the column is an int
                allIntegers = all(i.is_integer() for i in df[column].dropna())

                if allIntegers:
                    # this is an Integer column
                    ordered_columns[column] = {'type': get_int_type(maxVal), 'length': maxVal}
                    print(f"int, {str(maxVal):{col_spacing-5}}: {column} : ({df[column].dtype})")
                    #df[df[column].fillna(0) != 0.0][column].astype(int)
                else:
                    # this is a Float column
                    ordered_columns[column] = {'type': get_float_type(maxVal), 'length': maxVal}
                    print(f"{df[column].dtype}, {str(maxVal):{col_spacing-5}}: {column}")
            else:
                # These types were detected as integers during loading of the file.
                if is_int64_dtype(df[column]) or is_integer(df[column]):
                    ordered_columns[column] = {'type': get_int_type(maxVal), 'length': maxVal}
                    print(f"int, {str(maxVal):{col_spacing-5}}: {column}")
                else:
                    unknown = "???"
                    print(f"{unknown:{col_spacing}}: {column}")
        # Handling of Strings
        else:
            # Look for values that look like dates in 2018/01/01 or 01/01/2018 form
            patterns = [
                re.compile('^\d{1,2}[-/]\d{1,2}[-/]20\d\d$'),
                # re.compile('^\d{1,2}[-/]\d{1,2}[-/]\d{1,4}$'),
                re.compile('^20\d\d[-/]\d{1,2}[-/]\d{1,2}$')
                # re.compile('^\d{1,4}[-/]\d{1,2}[-/]\d{1,2}$')
            ]

            foundDate = False
            for pattern in patterns:
                if any(i == True for i in df[column].str.contains(pattern)):
                    foundDate = True

            foundBool = False
            try:
                maxVal = str(int(df[column].dropna().str.len().max()))
            except:
                # Could be boolean?
                # if "otc" in column:
                #     import pdb; pdb.set_trace()
                # if all(i.lower == "false" or i.lower() == "true" for i in df[column].dropna()):
                if any(type(i) == bool for i in df[column].dropna()):
                    maxVal = 0
                else:
                    maxVal = 0

            if foundDate:
                ordered_columns[column] = {'type': "DATE", 'length': maxVal}
                print(f"Date, {maxVal:{col_spacing-6}}: {column}")
            # elif foundBool:
            #     ordered_columns[column] = {'type': "BOOL", 'length': maxVal}
            #     print(f"Bool, {maxVal:{col_spacing-6}}: {column}")
            else:
                ordered_columns[column] = {'type': f"VARCHAR({int(maxVal)*varchar_factor})", 'length': maxVal}
                print(f"String, {maxVal:{col_spacing-8}}: {column}")

    print("-------------------------------------")
    print(f"Total columns are: {len(df.columns)}")
    print("-------------------------------------")

    if sql:
        create_table_sql(ordered_columns, table_name)

@click.command()
@click.option('--source-table-name', '-s', required=True, type=click.STRING, help="")
@click.option('--destination-table-name', '-d', required=True, type=click.STRING, help="")
@click.pass_context
def copy_table(ctx, source_table_name, destination_table_name):
    """
    Create a copy of a source table.
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    loader.connect(**ctx.obj['db_credentials'])
    loader.copy_table(source_table_name, destination_table_name)

@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.pass_context
def drop_table(ctx, table_name):
    """
    Drop a table
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    DROP_TABLE_DDL = f"DROP TABLE {table_name}"
    loader.connect(**ctx.obj['db_credentials'])
    loader._submit_single_q(DROP_TABLE_DDL)
    print(f"Dropped table {table_name}")


@click.command()
@click.option('--table-name', '-t', required=True, type=click.STRING, help="")
@click.option('--encoding', '-e', required=False, default="ASCII", type=click.STRING, help="")
@click.option('--column', '-c', required=True, type=click.STRING, help="")
@click.option('--count-only', '-o', required=False, default=True, type=click.BOOL, help="")
@click.pass_context
def check_encoding(ctx, table_name, encoding, column, count_only):
    """
    Check table column encodings.  Useful for finding multibyte data, which requires utf8, rather than latin1.
    """
    loader = BaseLoader(warnings=ctx.obj['warnings'])
    print(f"Checking table: {table_name}")

    columns = column.strip().split(",")

    max_col_len = max([ len(c) for c in columns ])
    if count_only:
        print(f"{'Column:':{max_col_len}s} Count:")

    for col in columns:
        q = COLUMN_ENCODING_DML.format(table_name=table_name, encoding=encoding, column=col)
        loader.connect(**ctx.obj['db_credentials'])
        r = loader._query(q)

        if count_only:
            print(f"{col:{max_col_len}s} {len(r)}")
        else:
            print(col)
            for entry in r:
                print(entry)

# deprecate, this has been moved under product loader
#
# @click.command()
# @click.option('--sql', '-s', required=True, type=click.File('rb'), help="SQL query file")
# @click.option('--left-table-name', '-l', required=True, type=click.STRING, help="")
# @click.option('--right-table-name', '-r', required=True, type=click.STRING, help="")
# @click.pass_context
# def delta(ctx, sql, left_table_name, right_table_name):
#     """
#     Delta between med device complete staging and prod table.  Update prod table with changes.
#     """
#     sql = sql.read().decode("utf-8")
#     print(sql)

#     loader = DeltaBaseLoader(warnings=ctx.obj['warnings'])
#     loader.connect(**ctx.obj['db_credentials'])
#     loader.delta_table(sql, left_table_name, right_table_name, ctx.obj['batch_size'], ctx.obj['throttle_size'], ctx.obj['throttle_time'])


tools.add_command(invalid_chars)
tools.add_command(x2c)
tools.add_command(preprocess)
tools.add_command(load)
tools.add_command(create_table)
tools.add_command(copy_table)
tools.add_command(drop_table)
tools.add_command(check_encoding)
# tools.add_command(delta)
