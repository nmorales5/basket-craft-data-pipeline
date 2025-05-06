"""
1. Import necessary libraries
2. Load MySQL and Postgres connection details
3. Build connection strings and create database engines
4. Read website_sessions table from MySQL for Dec 2023
5. Write dataframe to website_sessions table in Postgres (raw schema)
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

# %%
# Build connection strings
mysql_conn_str = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}'
pg_conn_str = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}'

# %%
# Create database engines
mysql_engine = create_engine(mysql_conn_str)
pg_engine = create_engine(pg_conn_str)

# %%
# Read website_sessions data for December 2023 from MySQL
query = """
SELECT *
FROM website_sessions
WHERE created_at BETWEEN '2023-12-01' AND '2023-12-31 23:59:59';
"""
df = pd.read_sql(query, mysql_engine)

# %%
# Write DataFrame to Postgres in the raw schema
from sqlalchemy import text

with pg_engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE raw.website_sessions"))

df.to_sql('website_sessions', pg_engine, schema='raw', if_exists='append', index=False)

# %%
print(f'{len(df)} records loaded into raw.website_sessions table in Postgres.')
