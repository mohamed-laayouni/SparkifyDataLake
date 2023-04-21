import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Preliminary check to drop any existing tables, executes the DROP statements in sql_queries.py

    Args:
        cur: psycopg2 connection cursor method
        conn: psycopg2 connection method, takes in a connection string
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    
    Executes all the CREATE statement in sql_queries.py

    Args:
        cur: psycopg2 connection cursor method
        conn: psycopg2 connection method, takes in a connection string
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()