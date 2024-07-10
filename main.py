# pulls data from Oracle database and converts it to pandas dataframe


import pandas as pd
import cx_Oracle
import os
from datetime import datetime


def pull_data(hospital_connection_string: str, number_of_classes: int):
    """pulls data from a hospital database"""

    # initialize the connection object
    try:
        # create a connection object
        conn = cx_Oracle.connect(hospital_connection_string)
        # get a cursor object from the connection
        cur = conn.cursor()
    # raise error if connection fails
    except Exception as err:
        print('Error while connecting to the db')
        print(err)
    finally:
        if conn:
            print('db hospital socket open..')
            # define the query parts that are needed to compose the query
            if number_of_classes == 2:
                with open(r"sql_feed_2class.sql") as sql_feed:
                    query = sql_feed.read()
            elif number_of_classes == 3:
                with open(r"sql_feed_3class.sql") as sql_feed:
                    query = sql_feed.read()
            # run query
            query_result = cur.execute(query).fetchall()
            # get query columns
            query_columns = [row[0] for row in cur.description]
            # close the cursor object to avoid memory leaks
            cur.close()
            # close the connection object
            conn.close()
            print('db hospital socket closed..\n')

    return query_columns, query_result


usr = [YOUR_ORACLE_SCHEMA_USERNAME]  # get schema user name
psswrd = [YOUR_ORACLE_SCHEMA_PASSWORD]  # get schema password
local_host = [YOUR_ORACLE_LOCALHOST]  # get schema local host
local_host = [YOUR_ORACLE_SERVICE_NAME]  # get schema service name


def create_df(hospital: str, number_of_classes: int):
    # compose connection string to database
    connection_string = usr  + "/" + psswrd  + "@" + local_host + "/" + service_name

    z = pull_data(connection_string, number_of_classes)  # pull data from datapool
    start_time = z[2]

    # convert the list of query results to pandas dataframe
    # df = pd.DataFrame(z[1]).dropna()
    df = pd.DataFrame(z[1])

    # add headers to created pandas dataframe
    df.columns = [x.lower() for x in z[0]]

    print('df_pulled\n')

    return df
