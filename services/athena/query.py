import awswrangler as wr
import pandas as pd
import boto3

df = wr.athena.read_sql_query(
    sql="SELECT * FROM train LIMIT 10",
    database="riftrewindmatchgluedb",
    boto3_session=boto3.Session(region_name='us-west-2')
)

print(df.head())


