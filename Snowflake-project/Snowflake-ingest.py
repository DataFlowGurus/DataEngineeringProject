import snowflake.connector
import pandas as pd

#Method #1 to connect with snowflake

con = snowflake.connector.connect(
    user='MAHESH1011',
    password='1234@Home',
    account='uwb81215',
    warehouse='TESTWH',
    database='DEMO_DB',   
    
)
# Query to test connection

df = con.cursor().execute("select * from customer_detail").fetch_pandas_all()
print(df)

