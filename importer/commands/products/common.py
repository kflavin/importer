

def data_loader(LoaderType, query, column_type_overrides, ctx, infile, table_name):
    loader = LoaderType(warnings=ctx.obj['warnings'])
    loader.column_type_overrides = column_type_overrides
    loader.connect(**ctx.obj['db_credentials'])
    loader.csv_loader(query, table_name, infile, ctx)

def parseInt(value):
    try:
        newvalue = int(value)
    except ValueError as e:
        newvalue = value
    return newvalue