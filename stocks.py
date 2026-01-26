# Import Packages
import os
import sys
import json
import time

import pandas as pd
import gspread
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# Get Date
now = datetime.now()
year = now.year
month = now.strftime("%b").lower()  # jan, feb, mar

table_suffix = f"{year}_{month}"
table_suffix

# Initialize BigQuery client
client = bigquery.Client(project='crypto-stocks-01')

# Define the scope for Google Sheets and BigQuery
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/cloud-platform'
]

# Load credentials from environment variable
credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)

# Initialize Google Sheets client
gc = gspread.authorize(creds)

# Open the Google Sheet by URL
spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1mcfemheGKOexESrRbscldcfhMNgB7ESwAg8j_uobHf8')
worksheet = spreadsheet.sheet1  # Select the first sheet

# Get the total number of rows with data
all_records = worksheet.get_all_records()
num_rows = len(all_records)

# Check if there are more than 30 rows of data
if num_rows <= 41:
    print(f"Only {num_rows} rows found. Exiting without processing.")
    sys.exit()  # Exit the script if 30 or fewer rows are found

# Extract Data, Convert to DataFrame
df = pd.DataFrame(worksheet.get('A2:Z41'), columns=worksheet.row_values(1))

# Original Data
data = df.copy()

# Rename Variables
data.columns = [
    "timestamp",
    "name",
    "last",
    "high",
    "low",
    "chg_",
    "chg_%",
    "vol_",
    "time"
]

# Standardize Column Names
data.columns = data.columns.str.lower().str.replace(' ', '_').str.replace(r'[()]', '', regex=True)

# Define Table ID
table_id = f"data-storage-485106.investing.stocks_{table_suffix}"

if now.day == 1:
    try:
        prev_month_date = now.replace(day=1) - timedelta(days=1)
        prev_table_suffix = f"{prev_month_date.year}_{prev_month_date.strftime('%b').lower()}"
        prev_table_id = f"data-storage-485106.investing.stocks_{prev_table_suffix}"
        
        try:
            prev_data = client.query(
                f"SELECT * FROM `{prev_table_id}`"
            ).to_dataframe()
            bigdata = pd.concat([prev_data, bigdata], ignore_index=True)
            print(f"Appended {len(prev_data)} rows from previous month table.")
        except NotFound:
            print("No previous month table found, skipping append.")
        
        job = client.load_table_from_dataframe(
            bigdata,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        print(f"All data loaded into {table_id}, total rows: {len(bigdata)}")

    except Exception as e:
        print(f"Error during 1st-of-month load: {e}")

else:
    # 🔥 NORMAL WORKFLOW (this was missing)
    job = client.load_table_from_dataframe(
        bigdata,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Normal load completed into {table_id}, rows: {len(bigdata)}")

# Define SQL Query to Retrieve Open Weather Data from Google Cloud BigQuery
sql = (f"""
        SELECT *
        FROM `{table_id}`
       """)
    
# Run SQL Query
data = client.query(sql).to_dataframe()

# Check Shape of data from BigQuery
print(f"Shape of dataset from BigQuery : {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=[
    'timestamp', 
    'name', 
    'last', 
    'high', 
    'low', 
    'chg_', 
    'chg_%', 
    'vol_', 
    'time']).sum()
    
# Remove Duplicate Records
data.drop_duplicates(subset=[
    'timestamp', 
    'name', 
    'last', 
    'high', 
    'low', 
    'chg_', 
    'chg_%', 
    'vol_', 
    'time'], inplace=True)

# Define the dataset ID and table ID
dataset_id = 'investing'
table_id = f"stocks_{table_suffix}"
    
# Define the table schema for new table
schema = [
        bigquery.SchemaField("timestamp", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("last", "STRING"),
        bigquery.SchemaField("high", "STRING"),
        bigquery.SchemaField("low", "STRING"),
        bigquery.SchemaField("chg_", "STRING"),
        bigquery.SchemaField("chg_%", "STRING"),
        bigquery.SchemaField("vol_", "STRING"),
        bigquery.SchemaField("time", "STRING"),
    ]
    
# Define the table reference
table_ref = client.dataset(dataset_id).table(table_id)
    
# Create the table object
table = bigquery.Table(table_ref, schema=schema)

try:
    # Create the table in BigQuery
    table = client.create_table(table)
    print(f"Table {table.table_id} created successfully.")
except Exception as e:
    print(f"Table {table.table_id} failed")

# Define the BigQuery table ID
table_id = f"data-storage-485106.investing.stocks_{table_suffix}"

# Load the data into the BigQuery table
job = client.load_table_from_dataframe(data, table_id)

# Wait for the job to complete
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)
    
# Return Data Info
print(f"Data {data.shape} has been successfully retrieved, saved, and appended to the BigQuery table.")

# Exit 
print(f'Stocks Data Export to Google BigQuery Successful')














