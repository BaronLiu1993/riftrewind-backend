import awswrangler as wr
import pandas as pd

sql = """

"""
"""
df: pd.DataFrame = wr.athena.read_sql_query(
    sql=sql,
    database="your_glue_db",
    workgroup="primary",               
    ctas_approach=False,             
    chunksize=None                    
)
"""


