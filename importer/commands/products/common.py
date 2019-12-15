

def data_loader(LoaderType, query, column_type_overrides, ctx, infile, table_name):
    loader = LoaderType(warnings=ctx.obj['warnings'])
    loader.column_type_overrides = column_type_overrides
    loader.connect(**ctx.obj['db_credentials'])
    loader.csv_loader(query, table_name, infile, ctx)


# The call to float() is because pandas will convert int column to floats, if there are any null values.
def parseIntOrNone(value):
    if value:
        try:
            newval = int(float(value))
        except ValueError as e:
            newval = value
    else:
        return None
    return newval


def parseInt(value):
    try:
        newvalue = int(value)
    except ValueError as e:
        newvalue = value
    return newvalue

# def parseIntOrNone(value):
#     try:
#         return int(float(value))
#     except Exception:
#         return None