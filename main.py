import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import subprocess
import psycopg2
import requests
from io import StringIO
from IPython.utils import io # --> Hide specific cell output
import string
import numpy as np
from pandas.tseries.offsets import  MonthEnd
import random
import datetime

print ('Libraries imported successfully')

# Update system packages
update_command = "sudo apt update"
subprocess.run(update_command, shell=True)

# Install PostgreSQL
print ('Installing PostgreSQL...')
install_command = "sudo apt install postgresql postgresql-contrib"
subprocess.run(install_command, shell=True)
print ('PostgreSQL installed successfully')

# Configuration data
postgres_status_command = "sudo service postgresql status"
postgres_start_command = "sudo service postgresql start"

subprocess.run(postgres_status_command, shell=True)
subprocess.run(postgres_start_command, shell=True)
subprocess.run(postgres_status_command, shell=True)

db_name = 'your_db_name'
db_user = 'your_username'
db_password = 'your_password'
db_host = 'localhost'
db_port = '5432'

# Drop database if exists
drop_db_command = f"sudo -u postgres psql -c \"DROP DATABASE IF EXISTS {db_name};\""
subprocess.run(drop_db_command, shell=True)

# Create a database user with password
create_user_command = f"sudo -u postgres psql -c \"CREATE USER {db_user} WITH PASSWORD '{db_password}';\""
subprocess.run(create_user_command, shell=True)

# Create a database
create_db_command = f"sudo -u postgres psql -c \"CREATE DATABASE {db_name} OWNER {db_user};\""
subprocess.run(create_db_command, shell=True)

# Grant privileges to the user on the database
grant_privileges_command = f"sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user};\""
subprocess.run(grant_privileges_command, shell=True)

print(f"PostgreSQL database '{db_name}' and user '{db_user}' created successfully.")

# Create a connection to the PostgreSQL server
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cur = conn.cursor()

# Create a table and insert data from online CSV files
def create_table_from_csv(url, table_name):
    response = requests.get(url)
    if response.status_code == 200:
        csv_data = response.text
        df = pd.read_csv(StringIO(csv_data))
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' created successfully and data inserted.")
    else:
        print(f"Failed to retrieve data from URL: {url}")

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# URLs of CSV files for table creation and data insertion
csv_urls = [
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_customers_dataset.csv?raw=true', 'table': 'customers'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_order_items_dataset.csv?raw=true', 'table': 'order_items'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_order_payments_dataset.csv?raw=true', 'table': 'order_payments'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_order_reviews_dataset.csv?raw=true', 'table': 'order_reviews'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_orders_dataset.csv?raw=true', 'table': 'orders'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_products_dataset.csv?raw=true', 'table': 'products'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/olist_sellers_dataset.csv?raw=true', 'table': 'sellers'},
    {'url': 'https://github.com/angeanto/ai_and_beers_hacking_data_and_analytics/blob/main/data/employees.csv?raw=true', 'table': 'employees'}
]

for file_info in csv_urls:
    create_table_from_csv(file_info['url'], file_info['table'])

# Read tables into Pandas DataFrames
def read_table_to_dataframe(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    return df

# Store tables in DataFrames
dataframes = {}
for file_info in csv_urls:
    table_name = file_info['table']
    dataframes[table_name] = read_table_to_dataframe(table_name)

print(""" Database tables created successfully. Let's build some analytics stuff.""")

# Function to execute SQL queries and fetch data
def execute_sql_query(sql_query):
    try:
        cur.execute(sql_query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        conn.commit()
        return pd.DataFrame(rows, columns=columns)

    except psycopg2.Error as e:
        print("Error:", e)
        conn.rollback()  # Rollback the transaction in case of error
        
# Store all tables to dataframes, we hide the output.
with io.capture_output() as captured:
    o_list_order_customer = read_table_to_dataframe('customers')
    o_list_order_items = read_table_to_dataframe('order_items')
    o_list_order_payments = read_table_to_dataframe('order_payments')
    o_list_order_reviews = read_table_to_dataframe('order_reviews')
    o_list_orders = read_table_to_dataframe('orders')
    o_list_products = read_table_to_dataframe('products')
    o_list_sellers = read_table_to_dataframe('sellers')
    employees = read_table_to_dataframe('employees')