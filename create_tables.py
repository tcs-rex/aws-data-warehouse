# import libraries and query lists
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Function responsible for dropping RedShift sql tables.

    This function loops through and executes the queries imported from sql_queries.py.
    Tables in AWS RedShift are dropped/deleted if they exist).

    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Function responsible for creating RedShift sql tables.

    This function loops through and executes the queries imported from sql_queries.py,
    to create tables in AWS RedShift.

    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function - starting point of the script.

    This function parses the configuration file for necessary info required to connect to the 
    AWS RedShift database. Once the connection is made and a cursor created, the above noted 
    query execution functions are called. Finally, the database connection is closed.

    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()