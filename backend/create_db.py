import os
# from importer.loaders.npi import NpiLoader
import mysql.connector as connector
from importer.sql.npi_create_clean import CREATE_TABLE_SQL


def handler(event, context):
    table_name = event.get('table_name', 'npi')
    database = event.get('database')

    # print("Creating database '{os.environ['db_schema']}' and table '{table_name}'...")
    # npi_loader = NpiLoader()
    # npi_loader.connect(user=os.environ['db_user'],
    #                 host=os.environ['db_host'], 
    #                 password=os.environ['db_password'], 
    #                 database=os.environ['db_schema'],
    #                 set_db=False)
    # npi_loader.create_database()
    # npi_loader.create_table(table_name)



    cnx = connector.connect(user=os.environ['db_user'], 
                            password=os.environ['db_password'], 
                            host=os.environ['db_host'])
    cursor = cnx.cursor()


    create_database_sql = f"CREATE DATABASE IF NOT EXISTS {database}"
    cursor.execute(create_database_sql)
    cnx.database = database

    create_table_sql = CREATE_TABLE_SQL.format(table_name=table_name)
    cursor.execute(create_table_sql)
    cnx.commit()
    
    print("Done!")


    