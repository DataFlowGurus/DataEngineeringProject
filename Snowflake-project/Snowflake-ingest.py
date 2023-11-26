import snowflake.connector
import pandas as pd
import argparse

#Method #1 to connect with snowflake
def main(params):
    con = snowflake.connector.connect(
        user=params.user,
        password=params.password,
        account='uwb81215',
        warehouse='TESTWH',
        database='DEMO_DB',   
        
    )

    
# Query to test connection

    df = con.cursor().execute("select * from customer_detail").fetch_pandas_all()
    print(df)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for Snowflake')
    parser.add_argument('--password', required=True, help='password for Snowflake')
    #parser.add_argument('--host', required=True, help='host for postgres')
    #parser.add_argument('--port', required=True, help='port for postgres')
    #parser.add_argument('--db', required=True, help='database name for postgres')
    #parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    #parser.add_argument('--url', required=True, help='url of the file')

    args = parser.parse_args()

    main(args)



