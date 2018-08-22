import os
import mysql.connector as connector
from pprint import pprint
import boto3
import sys
from importer.sql.npi import INSERT_NEW_FILE, GET_FILES


class DBHelper(object):
    def __init__(self, region, config=None):
        ssm = boto3.client('ssm', region_name=region)

        if config:
            self.config = config
        else:   
            self.config = {
                'user': ssm.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value'],
                'password': ssm.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value'],
                'host': ssm.get_parameter(Name='db_host')['Parameter']['Value'],
                'database': ssm.get_parameter(Name='db_schema', WithDecryption=True)['Parameter']['Value']
            }

    def imports_ready(self, table_name, period, limit):
        print("Connecting to DB")

        p = "m" if period.lower() == "monthly" else "w"

        cnx = connector.connect(**self.config)
        cursor = cnx.cursor(buffered=True)
        q = GET_FILES.format(table_name=table_name, period=p, limit=limit)
        cursor.execute(q)
        
        # If there are any files to import, return true
        if cursor.rowcount > 0:
            return True
        
        return False

    def add_to_db(self, url, table_name, p):
        print("Connecting to DB")
        cnx = connector.connect(**self.config)
        cursor = cnx.cursor()
        cols = "url, period"
        values = "%(url)s, %(period)s"
        query = INSERT_NEW_FILE.format(table_name=table_name, cols=cols, values=values)
        
        print("Query")
        print(query)

        data = {
            "url": url,
            "period": p
        }

        print("Query is: {}".format(query))

        cursor.execute(query, data)
        cnx.commit()
        cursor.close()
        cnx.close()


# Test from the CLI
if __name__ == "__main__":
    print("Add to db")
    # add_to_db("myurl", "npi_import_log")
    imports_ready("npi_import_log", "weekly", 1)
