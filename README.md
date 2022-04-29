# data-engineering

#snowflake connection
import snowflake.connector
cnn = snowflake.connector.connect(
user='SAIPRIYA',
password='Priya@123',
account='kw25469.ap-south-1.aws'
)
cs = cnn.cursor()
try:
cs.execute('select current_version()')
row = cs.fetchone()
print(row[0])
finally:
cs.close()
cnn.close()

#create database and schema
import snowflake.connector
cnn = snowflake.connector.connect(
        user='SAIPRIYA',
        password='Priya@123',
        account='kw25469.ap-south-1.aws'
        )
cs = cnn.cursor()
try:
    cs.execute('select current_version()')
    row = cs.fetchone()
    print(row[0])
    print('creating warehouse..')
    sql = "CREATE WAREHOUSE IF NOT EXISTS project_warehouse"
    cs.execute(sql)
    print('creating database..')
    sql = "CREATE DATABASE IF NOT EXISTS project_database"
    cs.execute(sql)
    print('using database..')
    sql = "USE DATABASE project_database"
    cs.execute(sql)
    print('creating schema..')
    sql = "CREATE SCHEMA IF NOT EXISTS project_schema"
    cs.execute(sql)
    print('creation complete.')
finally:
    cs.close()
cnn.close()

#data ingestion
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

print('opening..')
df = pd.read_csv("C:/Users/SaiMandapati/Downloads/energy_dataset.csv", sep=',', header=0, index_col=False)
df.reset_index(drop=True, inplace=True)
print(df.head(10))
print('opening snowflake..')
cnn = snowflake.connector.connect(
     user='SAIPRIYA',
     password='Priya@123',
     account='kw25469.ap-south-1.aws',
     warehouse='project_warehouse',
     database='project_database',
     schema='project_schema'
    )
success, nchunks, nrows, _ = write_pandas(cnn, df, 'PROJECT_TABLE1', quote_identifiers=True)
print(str(success) + ',' + str(nchunks) + ',' + str(nrows))
cnn.close()
print('done.')

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

print('opening..')
df = pd.read_csv("C:/Users/SaiMandapati/Downloads/weather_features.csv", sep=',', header=0, index_col=False)
df.reset_index(drop=True, inplace=True)
print(df.head(10))
print('opening snowflake..')
cnn = snowflake.connector.connect(
     user='SAIPRIYA',
     password='Priya@123',
     account='kw25469.ap-south-1.aws',
     warehouse='project_warehouse',
     database='project_database',
     schema='project_schema'
    )
success, nchunks, nrows, _ = write_pandas(cnn, df, 'PROJECT_TABLE2', quote_identifiers=False)
print(str(success) + ',' + str(nchunks) + ',' + str(nrows))
cnn.close()
print('done.')
