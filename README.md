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
