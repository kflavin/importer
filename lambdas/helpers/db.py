import os
import psycopg2
from pprint import pprint
import boto3
import sys
from importer.sql.npi import INSERT_NEW_FILE, GET_FILES


class DBHelper(object):
    def __init__(self, region, config=None):
        environment = os.environ.get('environment', 'dev')
        ssm = boto3.client('ssm', region_name=region)

        if config:
            self.config = config
            user = config['db_user']
            password = config['db_password']
            host = config['db_host']
            database = config['db_name']
        else:
            user = ssm.get_parameter(
                Name=f'/importer/{environment}/db_user', WithDecryption=True)['Parameter']['Value']
            password = ssm.get_parameter(
                Name=f'/importer/{environment}/db_password', WithDecryption=True)['Parameter']['Value']
            self.host = ssm.get_parameter(
                Name=f'/importer/{environment}/db_host')['Parameter']['Value']
            self.database = ssm.get_parameter(
                Name=f'/importer/{environment}/db_name', WithDecryption=True)['Parameter']['Value']

        self.connection_string = f'dbname={self.database} host={self.host} user={user} password={password}'
        self.cnx = psycopg2.connect(self.connection_string)
        self.cursor = self.cnx.cursor()

    def close(self):
        self.cursor.close()
        self.cnx.close()

    def files_ready(self, table_name, period, environment, limit):
        print(
            f"Connecting to host: {self.host} db: {self.database} table: ${table_name}, environment: {environment} limit: {limit}")

        p = "m" if period.lower() == "monthly" else "w"

        q = GET_FILES.format(table_name=table_name, period=p,
                             environment=environment, limit=limit)
        print(q.replace('\n', ' ').replace('\r', ''))
        self.cursor.execute(q)

        # If there are any files to import, return true
        if self.cursor.rowcount > 0:
            return True

        return False

    def add_to_db(self, url, table_name, p, environment):
        print("Add URL to DB")
        cols = "url, period, environment"
        values = "%(url)s, %(period)s, %(environment)s"
        query = INSERT_NEW_FILE.format(
            table_name=table_name, cols=cols, values=values)

        data = {
            "url": url,
            "period": p,
            "environment": environment
        }

        try:
            self.cursor.execute(query, data)
            self.cnx.commit()
        except psycopg2.Error as e:
            print(e)
            print("Integrity error, may have duplicate key, won't insert.")
            return False
        except Exception as e:
            print(e)
            return False

        return True


# Test from the CLI
if __name__ == "__main__":
    table_name = os.environ.get('npi_log_table_name', 'npi_import_logs')
    region = os.environ.get('aws_region', 'us-east-1')
    environment = os.environ.get('environment', "dev")
    url = "http://test-url"
    p = 'w'
    if not table_name or not region or not environment:
        print("Must provide npi_log_table_name, aws_region in ENV")
        sys.exit(0)

    db = DBHelper(region)
    print(db.add_to_db(url, table_name, p, environment))
    db.close()
