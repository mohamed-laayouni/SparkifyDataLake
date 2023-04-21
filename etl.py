import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the data from s3 into our staging tables, as defined in the copy_table_queries list in sql_queries.py

    Args:
        cur: psycopg2 connection cursor method
        conn: psycopg2 connection method, takes in a connection string
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    
    Executes the INSERT statement in sql_queries.py

    Args:
        cur: psycopg2 connection cursor method
        conn: psycopg2 connection method, takes in a connection string
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()