import snowflake.connector
import pandas as pd
import argparse
import os 
from pyarrow import *
import requests
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from pandas.io import sql
#method#2- sqlalchemy connection to connect snowflake


engine = create_engine(URL(
    account = 'uwb81215',
    user = 'MAHESH1011',
    password = '1234@Home',
    database = 'DEMO_DB',
    warehouse = 'TESTWH',
    schema ='public',
    role='ACCOUNTADMIN'
    
))

conn = engine.connect()



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
print(df.head(0))


#reading parquet file from web url and download 
url = input()
response = requests.head(url)
if response.status_code == 200:
    os.system(f"curl {url} -o output.parquet")
else:
    print("file is not avilable on below link :", '\n {url}')


# reading parquet file from weblink and load into snowflake table using sqlalchemy connection

df = pd.read_parquet(url,engine ='pyarrow')
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
print("File contain total row:{}".format(len(df)))
print("Do you want to append data in existing table:")
selection = input().upper()
if selection=='Y':
    df.to_sql(name='yello_taxi', con=conn, if_exists='append',index =False , chunksize = 16000)
else:
    df.to_sql(name='yello_taxi', con=conn, if_exists='replace' ,index =False , chunksize = 16000)

    

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



