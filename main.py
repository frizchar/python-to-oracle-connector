# pulls data from Oracle database and converts it to pandas dataframe


import pandas as pd
import cx_Oracle


def pull_data(connection_string: str):
    """
    function pulls data from Oracle database
    """

    # initialize the connection object
    try:
        # create a connection object
        conn = cx_Oracle.connect(connection_string)
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
            with open(r"sql_script.sql") as sql_feed:
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


def create_df(usr: str, psswrd: str, local_host: str, service_name: str):
    """
    function generates connection string to Oracle database and then calls function pull_data()

    function arguments:
    usr := [YOUR_ORACLE_SCHEMA_USERNAME]  # Oracle schema username
    psswrd := [YOUR_ORACLE_SCHEMA_PASSWORD]  # Oracle schema password
    local_host := [YOUR_ORACLE_LOCALHOST]  # Oracle schema local host
    service_name := [YOUR_ORACLE_SERVICE_NAME]  # Oracle schema service name
    """

    connection_string = usr  + "/" + psswrd  + "@" + local_host + "/" + service_name

    z = pull_data(connection_string)  # pull data from datapool

    # convert the list of query results to pandas dataframe
    # df = pd.DataFrame(z[1]).dropna()
    df = pd.DataFrame(z[1])

    # add headers to created pandas dataframe
    df.columns = [x.lower() for x in z[0]]

    print('df_pulled\n')

    return df

if __name__ == "__main__":
    df = create_df('[YOUR_ORACLE_SCHEMA_USERNAME]',
                   '[YOUR_ORACLE_SCHEMA_PASSWORD]',
                   '[YOUR_ORACLE_LOCALHOST]',
                   '[YOUR_ORACLE_SERVICE_NAME]')

    print(df.head())
