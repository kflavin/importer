import click
import os
import logging
import sys
import re
from numpy import dtype
import pandas as pd
from pandas.api.types import (is_string_dtype, is_int64_dtype, is_integer,
                            is_numeric_dtype, is_float_dtype)

# from importer.loaders.hdm import HdmLoader

logger = logging.getLogger(__name__)

@click.group()
# @click.option('--batch-size', '-b', type=click.INT, default=1000, help="Batch size, only applies to weekly imports.")
# @click.option('--throttle-size', type=click.INT, default=10000, help="Sleep after this many inserts.")
# @click.option('--throttle-time', type=click.INT, default=3, help="Time (s) to sleep after --throttle-size.")
@click.pass_context
#def csv(ctx, batch_size, throttle_size, throttle_time):
def csv(ctx):
    ctx.ensure_object(dict)
    # ctx.obj['batch_size'] = batch_size
    # ctx.obj['throttle_size'] = throttle_size
    # ctx.obj['throttle_time'] = throttle_time


@click.command()
# @click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with NPI data")
# @click.option('--step-load', '-s', nargs=2, type=click.INT, help="Use the step loader.  Specify a start and end line.")
@click.option('--infile', '-i', required=True, type=click.STRING, help="CSV file with table data")
@click.option('--table-name', '-t', default="[TABLE_NAME]", required=True, type=click.STRING, help="Table name.")
@click.option('--col-spacing', '-s', default=20, required=True, type=click.INT, help="Spacing between columns.")
@click.pass_context
def create_table(ctx, infile, table_name, col_spacing):
    """
    Display SQL table create command
    """
    # batch_size = ctx.obj['batch_size']
    # throttle_size = ctx.obj['throttle_size']
    # throttle_time = ctx.obj['throttle_time']

    df = pd.read_csv(infile)

    count = 0
    for column in df.columns:
        #print(df[column].dtype)
        count += 1
        sys.stdout.write(f"{str(count):3} ")

        if df[column].isna().all():
            print("{:{col_spacing}}: {}".format("No values", column, col_spacing=col_spacing))
            continue

        #print(df[column].dtype)
        if is_numeric_dtype(df[column]):
            if is_float_dtype(df[column]):
                # Pandas stores numerical columns with null values as floats.  We
                # need to do some extra work to determine if the column is an int
                allIntegers = any(i.is_integer() for i in df[column].dropna())

                if allIntegers:
                    maxVal = None
                    allVals = [i for i in df[column].dropna()]
                    if allVals:
                        maxVal = max(allVals)

                    if not maxVal:
                        # We shouldn't make it here
                        print("{0:{col_spacing}}: {1}".format('No values?', column,
                                                    col_spacing=col_spacing))
                    else:
                        print(f"int, {str(maxVal):{col_spacing-5}}: {column}")
                    #df[df[column].fillna(0) != 0.0][column].astype(int)
                else:
                    print(f"{str(df[column].dtype):{col_spacing}}: {column}")
            else:
                #print("{:{col_spacing}}: {column}")
                if is_int64_dtype(df[column]) or is_integer(df[column]):
                    maxVal = None
                    allVals = [i for i in df[column].dropna()]
                    if allVals:
                        maxVal = max(allVals)
                    print(f"int, {str(maxVal):{col_spacing-5}}: {column}")
                else:
                    unknown = "???"
                    print(f"{unknown:{col_spacing}}: {column}")
        else:
            #print('string')
            patterns = [
                re.compile('^\d{1,2}[-/]\d{1,2}[-/]\d{1,4}$'),
                re.compile('^\d{1,4}[-/]\d{1,2}[-/]\d{1,2}$')
            ]

            foundDate = None
            for pattern in patterns:
                if any(i == True for i in df[column].str.contains(pattern)):
                    foundDate = True

            if foundDate:
                print(f"Date, {str(int(df[column].str.len().max())):{col_spacing-6}}: {column}")
            else:
                print(f"String, {str(int(df[column].str.len().max())):{col_spacing-8}}: {column}")


    print(f"Total columns are: {len(df.columns)}")


csv.add_command(create_table)