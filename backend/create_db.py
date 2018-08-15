import os
import mysql.connector as connector
import boto3
from importer.sql.npi_create_clean import CREATE_TABLE_SQL
from importer.sql.npi_import_log_create import CREATE_NPI_IMPORT_LOG_SQL


def handler(event, context):
    table_name = event.get('table_name', 'npi')
    log_table_name = event.get('log_table_name', 'npi_import_log')
    database = event.get('database')
    client = boto3.client('ssm')

    args = {
        'user': client.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value'],
        'password': client.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value'],
        'host': client.get_parameter(Name='db_host')['Parameter']['Value']
    }

    cnx = connector.connect(**args)
    cursor = cnx.cursor()


    create_database_sql = f"CREATE DATABASE IF NOT EXISTS {database}"
    cursor.execute(create_database_sql)
    cnx.database = database

    # Create NPI table
    create_table_sql = CREATE_TABLE_SQL.format(table_name=table_name)
    cursor.execute(create_table_sql)

    # Create NPI import log table
    create_log_table_sql = CREATE_NPI_IMPORT_LOG_SQL.format(table_name=log_table_name)
    cursor.execute(create_log_table_sql)

    cnx.commit()
    
    print("Done!")


    