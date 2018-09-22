import os
import mysql.connector as connector
from pprint import pprint
import boto3
import sys
from importer.sql.npi import INSERT_NEW_FILE, GET_FILES


class DBHelper(object):
    def __init__(self, region, config=None):
        stage = os.environ.get('stage', 'dev')
        ssm = boto3.client('ssm', region_name=region)

        if config:
            self.config = config
        else:   
            self.config = {
                'user': ssm.get_parameter(Name=f'/importer/{stage}/db_user', WithDecryption=True)['Parameter']['Value'],
                'password': ssm.get_parameter(Name=f'/importer/{stage}/db_password', WithDecryption=True)['Parameter']['Value'],
                'host': ssm.get_parameter(Name=f'/importer/{stage}/db_host')['Parameter']['Value'],
                'database': ssm.get_parameter(Name=f'/importer/{stage}/db_schema', WithDecryption=True)['Parameter']['Value']
            }

    def files_ready(self, table_name, period, environment, limit):
        print(f"Connecting to host: {self.config.get('host')} db: {self.config.get('database')} table: ${table_name}, environment: {environment} limit: {limit}")

        p = "m" if period.lower() == "monthly" else "w"

        cnx = connector.connect(**self.config)
        cursor = cnx.cursor(buffered=True)
        q = GET_FILES.format(table_name=table_name, period=p, environment=environment, limit=limit)
        print(q.replace('\n', ' ').replace('\r', ''))
        cursor.execute(q)
        
        # If there are any files to import, return true
        if cursor.rowcount > 0:
            return True
        
        return False

    def add_to_db(self, url, table_name, p, environment):
        print("Connecting to DB")
        cnx = connector.connect(**self.config)
        cursor = cnx.cursor()
        cols = "url, period, environment"
        values = "%(url)s, %(period)s, %(environment)s"
        query = INSERT_NEW_FILE.format(table_name=table_name, cols=cols, values=values)

        data = {
            "url": url,
            "period": p,
            "environment": environment
        }

        try:
            cursor.execute(query, data)
            cnx.commit()
        except connector.IntegrityError as e:
            print(e)
            print("Integrity error, may have duplicate key, won't insert.")
            cursor.close()
            cnx.close()
            return False

        cursor.close()
        cnx.close()
        return True


# Test from the CLI
if __name__ == "__main__":
    print("Add to db")
    # add_to_db("myurl", "npi_import_log")
    imports_ready("npi_import_log", "weekly", 1)
