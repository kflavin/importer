import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import boto3
from importer.sql.npi import CREATE_NPI_TABLE, CREATE_NPI_IMPORT_LOG_TABLE


def handler(event, context):
    stage = event.get('stage', 'dev')
    table_name = event.get('table_name')
    log_table_name = event.get('log_table_name')
    database = event.get('database')
    client = boto3.client('ssm')

    user = client.get_parameter(
        Name=f'/importer/{stage}/db_user', WithDecryption=True)['Parameter']['Value']
    password = client.get_parameter(
        Name=f'/importer/{stage}/db_password', WithDecryption=True)['Parameter']['Value']
    host = client.get_parameter(
        Name=f'/importer/{stage}/db_host')['Parameter']['Value']

    # Create DB
    conn = psycopg2.connect(f'host={host} user={user} password={password}')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    create_database_sql = f"CREATE DATABASE {database}"
    cursor.execute(create_database_sql)
    cursor.close()
    conn.close()

    # Connect again with newly created DB
    conn = psycopg2.connect(
        f'dbname={database} host={host} user={user} password={password}')
    cursor = conn.cursor()

    # Create NPI table
    create_table_sql = CREATE_NPI_TABLE.format(table_name=table_name)
    cursor.execute(create_table_sql)

    # Create NPI import log table
    create_log_table_sql = CREATE_NPI_IMPORT_LOG_TABLE.format(
        table_name=log_table_name)
    cursor.execute(create_log_table_sql)

    conn.commit()
    cursor.close()
    conn.close()

    print("Done!")


# Test from the CLI
if __name__ == "__main__":
    print("Add tables to DB")
    event = {
        'stage': 'dev',
        'table_name': 'npis',
        'log_table_name': 'npi_import_logs',
        'database': 'importer_testing'
    }
    handler(event, {})
