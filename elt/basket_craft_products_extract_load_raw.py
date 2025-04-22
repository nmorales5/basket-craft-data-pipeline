"""
1. Import necessary libraries.
2. Load MySQL and Postgres connection details
3. Build connection strings and create database engines
4. Read products table from MySQL and load into a Dataframe
5. Write dataframe to products tables in PostGres (raw schema)
"""
# %%
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# %%
# load environment variables from .env file
load_dotenv()

# %%
# MySQL database connection details
mysql_user = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
mysql_host = os.environ['MYSQL_HOST']
mysql_db = os.environ['MYSQL_DB']

# Postgres database connection details
pg_user = os.environ['PG_USER']
pg_password = os.environ['PG_PASSWORD']
pg_host = os.environ['PG_HOST']
pg_db = os.environ['PG_DB']

pg_db
# %%



# %%
#Build connection strings
mysql_conn_str = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}'
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'

# %%
# create database engines
mysql_engine = create_engine(mysql_conn_str)
pg_engine = create_engine(pg_conn_str)
# %%
# read products table from MySQL
df = pd.read_sql('SELECT * FROM products', mysql_engine)
# %%
#df
# %%
# Write Dataframe to products table in Postgres
df.to_sql('products', pg_engine, schema = 'raw', if_exists='replace', index=False)
# %%
print(f'{len(df)} records loaded into Postgres products table.')
# %%
