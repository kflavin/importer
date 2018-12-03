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
    print(");")
    print("-- Be sure to verify the column values!")

logger = logging.getLogger(__name__)

@click.group()
@click.pass_context
def csv(ctx):
    ctx.ensure_object(dict)

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
@click.pass_context
def create_table(ctx, infile, table_name, col_spacing, varchar_factor, sql, encoding):
    """
    Display SQL table create command from a CSV file.
    """

    ordered_columns = OrderedDict()

    if infile.endswith(".xls") or infile.endswith(".xlsx"):
        print("Loading Excel file...")
        df = pd.ExcelFile(infile).parse()
    else:
        print("Loading CSV file...")
        df = pd.read_csv(infile, encoding=encoding)
    
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
                re.compile('^\d{1,2}[-/]\d{1,2}[-/]\d{1,4}$'),
                re.compile('^\d{1,4}[-/]\d{1,2}[-/]\d{1,2}$')
            ]

            foundDate = None
            for pattern in patterns:
                if any(i == True for i in df[column].str.contains(pattern)):
                    foundDate = True

            try:
                maxVal = str(int(df[column].dropna().str.len().max()))
            except:
                # Could be boolean?
                if any(type(i) == bool for i in df[column].dropna()):
                    maxVal = 0
                else:
                    maxVal = 0

            if foundDate:
                ordered_columns[column] = {'type': "DATE", 'length': maxVal}
                print(f"Date, {maxVal:{col_spacing-6}}: {column}")
            else:
                ordered_columns[column] = {'type': f"VARCHAR({int(maxVal)*varchar_factor})", 'length': maxVal}
                print(f"String, {maxVal:{col_spacing-8}}: {column}")

    print("-------------------------------------")
    print(f"Total columns are: {len(df.columns)}")
    print("-------------------------------------")

    if sql:
        create_table_sql(ordered_columns, table_name)
        

csv.add_command(invalid_chars)
csv.add_command(x2c)
csv.add_command(create_table)