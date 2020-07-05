import configparser
import psycopg2
from sql_queries import create_fact_dim_table_queries, drop_fact_dim_table_queries
from sql_queries import create_staging_table_queries, drop_staging_table_queries


def drop_staging_tables(cur, conn):
    for query in drop_staging_table_queries:
        cur.execute(query)
        conn.commit()

        
def drop_fact_dim_tables(cur, conn):
    for query in drop_fact_dim_table_queries:
        cur.execute(query)
        conn.commit()

        
def create_staging_tables(cur, conn):
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()

        
def create_fact_dim_tables(cur, conn):
    for query in create_fact_dim_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_staging_tables(cur, conn)
    drop_fact_dim_tables(cur, conn)
    create_staging_tables(cur, conn)
    create_fact_dim_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()