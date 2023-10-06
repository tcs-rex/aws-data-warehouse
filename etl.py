# import libraries and query containers
import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries, quality_queries, exploration_queries


def load_staging_tables(cur, conn):
    """
    Function copies/loads S3 Sparkify data into RedShift staging tables.

    This function loops through and executes the queries imported from sql_queries.py to load
    S3 data into AWS RedShift staging tables. The function also calculates and prints query
    execution times.

    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    
    for query in copy_table_queries:
        print("executing query: ",query[0:20],"...")
        start_time = time.time()
        cur.execute(query)
        conn.commit()
        end_time = time.time()
        iteration_time = end_time - start_time
        print(f"Elapsed time: {iteration_time} seconds")
        print("Completed executing query.\n")

def insert_tables(cur, conn):
    """
    Function inserts data from staging tables into the fact and dimension tables. 

    This function loops through and executes the queries imported from sql_queries.py to load
    select staging table data (transformed as necessary), into the fact and dimension 
    tables. The function also calculates and prints query execution times. 
    
    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    
    for query in insert_table_queries:
        print("executing query: ",query[0:20],"...")
        start_time = time.time()
        cur.execute(query)
        conn.commit()
        end_time = time.time()
        iteration_time = end_time - start_time
        print(f"Elapsed time: {iteration_time} seconds")
        print("Completed executing query.\n")

def quality_check_queries(cur, conn):
    """
    Function counts fact and dimension table rows as a quality check. 

    This function loops through and executes the queries imported from sql_queries.py to count 
    the number of rows present in the final fact and dimension tables, and the system data 
    loading errors table ("stl_load_errors"), if any. The function also calculates and prints 
    query execution times. 
    
    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    
    print("QUALITY CHECKS...")
    for query in quality_queries:
        cur.execute(query)
        print("executed query: ",query)
        for row in cur.fetchall():
            print(row)


def explore_queries(cur, conn):
    """
    Function executes optional data analytics queries. 

    This function loops through and executes the optional data analystics queries imported 
    from sql_queries.py. The function also calculates and prints query execution times, as well 
    as the query results.
    
    Args:
        cur (cursor): Database cursor. 
        conn (connection): Database connection.

    Returns:
        type: None.
    """
    
    print("\nEXPLORATION QUERIES...")
    for key, value in exploration_queries.items():
        print(f"executing query: {key}")
        cur.execute(value)
        for row in cur.fetchall():
            print(row)
        print("")

    
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    quality_check_queries(cur, conn)
    explore_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()