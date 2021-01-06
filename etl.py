import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load to staging tables using copy command from S3

    :params cur - psycopg cursor
    :type obj
    :params conn - psycopg connection to Redshift database
    :type obj
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Finished loading staging tables')


def insert_tables(cur, conn):
    """
    Load from staging tables to star schema using insert operations

    :params cur - psycopg cursor
    :type obj
    :params conn - psycopg connection to Redshift database
    :type obj
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('Finished inserting in star schema')


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