import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import time

# Set the path to your service account key JSON file
credentials_path = r"C:\Users\anand\Youtube\service-account-key.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

def load_csv_to_bigquery(table_name, csv_file):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        max_bad_records=100  # Allow up to 10 bad records
    )

    table_id = table_name

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
        print(job.state)

    print(job.result())

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

table_name = "ga4-bq-connector.DimTables.Vedantu_Channel_ID_Name_mapping"
csv_filename = r"C:\Users\anand\OneDrive\Documents\Intern_Project\Vedantu Channel List and Id - ChannelID mapping.csv" 

load_csv_to_bigquery(table_name, csv_filename)