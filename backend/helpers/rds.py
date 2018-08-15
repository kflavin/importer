import os
import mysql.connector as connector
from pprint import pprint
import boto3
import sys
from importer.sql.npi_import_log_insert import INSERT_NPI_IMPORT_LOG_QUERY

def add_to_db(url, table_name):
    ssm = boto3.client('ssm', region_name=os.environ['aws_region'])
    config = {
        'user': ssm.get_parameter(Name='db_user', WithDecryption=True)['Parameter']['Value'],
        'password': ssm.get_parameter(Name='db_password', WithDecryption=True)['Parameter']['Value'],
        'host': ssm.get_parameter(Name='db_host')['Parameter']['Value'],
        'database': ssm.get_parameter(Name='db_schema', WithDecryption=True)['Parameter']['Value']
    }

    print("Connecting to DB")
    cnx = connector.connect(**config)
    cursor = cnx.cursor()

    # cols = ""
    # columns = ['url']
    # for column in columns:
        # cols += "`{}`, ".format(column)
        # values += "%({})s, ".format(column)

    # cols = cols.rstrip().rstrip(",")
    # values = values.rstrip().rstrip(",")
    query = INSERT_NPI_IMPORT_LOG_QUERY.format(table_name=table_name, cols="`url`", values="%(url)s")
    print("Query")
    print(query)

    data = {
        "url": url
    }

    print("Query is: {}".format(query))

    cursor.execute(query, data)
    cnx.commit()
    cursor.close()
    cnx.close()


# Test from the CLI
if __name__ == "__main__":
    print("Add to db")
    add_to_db("myurl", "npi_import_log")


#     def __submit_batch(self, query, data):
#         if self.debug:
#             print(query)

#         tries = 3
#         count = 0

#         # Simple retry
#         while count < tries:
#             try:
#                 # cursor.execute(sql, (arg1, arg2))
#                 # Deadlock error here when too many processes run at once.  Implement back off timer.
#                 # mysql.connector.errors.InternalError: 1213 (40001): Deadlock found when trying to get lock; try restarting transaction
#                 self.cursor.executemany(query, data)
#                 self.cnx.commit()
#                 break
#             except mysql.connector.errors.InternalError as e:
#                 # print(self.cursor._last_executed)
#                 # print(self.cursor.statement)
#                 print("Rolling back...")
#                 self.cnx.rollback()
#                 count += 1
#                 print("Failed on try {count}/{tries}")
#                 if count < tries:
#                     print("Could not submit batch")
#                     raise

#         return self.cursor.rowcount


    