import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import time


project_id = "ga4-bq-connector"
dataset_id = "YoutubeTamil9to10"


# Function to load CSV to BigQuery table
def load_csv_to_bigquery(table_id, csv_file):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        max_bad_records=100  # Allow up to 100 bad records
    )

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

# Function to format date
def format_date(date_str, input_format='%Y%m%d', output_format='%Y%m%d'):
    # Parse the input date string with the input format
    parsed_date = pd.to_datetime(date_str, format=input_format, errors='coerce')

    # Format the date with the desired output format
    formatted_date = parsed_date.strftime(output_format)

    return formatted_date

# Set the path to your service account key JSON file
credentials_path = r"C:\Users\anand\Youtube\service-account-key.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Create a BigQuery client with explicit credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(project="ga4-bq-connector", credentials=credentials)

# Define the table mapping
table_mapping = {
    "p_channel_annotations_a1_vedTam": "Annotations",
    "p_playlist_province_a1_vedTam": "Playlist province",
    "p_channel_cards_a1_vedTam": "Cards",
    "p_channel_province_a2_vedTam": "Province",
    "p_channel_sharing_service_a1_vedTam": "vedantudp",
    "p_channel_end_screens_a1_vedTam": "End screens",
    "p_playlist_basic_a1_vedTam": "Playlist user activity",
    "p_playlist_playback_location_a1_vedTam": "Playlist playback locations",
    "p_playlist_device_os_a1_vedTam": "Playlist device and OS",
    "p_playlist_traffic_source_a1_vedTam": "Playlist traffic sources",
    "p_playlist_combined_a1_vedTam": "Playlist combined",
    "p_channel_combined_a2_vedTam": "Combined",
    "p_channel_demographics_a1_vedTam": "Demographics",
    "p_channel_subtitles_a2_vedTam": "Subtitles",
    "p_channel_playback_location_a2_vedTam": "Playback locations",
    "p_channel_device_os_a2_vedTam": "Device and OS",
    "p_channel_basic_a2_vedTam": "vidiq",
    "p_channel_traffic_source_a2_vedTam": "Traffic sources"
}

# Folder containing the files
folder_path = r"C:\Users\anand\OneDrive\Documents\Intern_Project\YoutubeCSVs1\token_vedantu_9and10"


# Iterate through files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        # Extract prefix from the file name and remove whitespace
        prefix = file_name.split("_")[0].strip()
        # Initialize table_name with a default value
        table_name = None
        for key, value in table_mapping.items():
            if prefix == value:
                table_name = key
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        print('Table id is:', table_id)
        if table_id:
            # Read the CSV file into a Pandas DataFrame
            file_path = os.path.join(folder_path, file_name)

            try:
                df = pd.read_csv(file_path)

                # Check if the DataFrame is not empty
                if df.empty:
                    print(f"The DataFrame is empty for {file_name}. Skipping further processing.")
                    continue

                # Extract date and channel_id from the DataFrame
                date_column_name = 'date'
                channel_id_column_name = 'channel_id'

                # Check if the columns exist in the DataFrame
                if date_column_name in df.columns and channel_id_column_name in df.columns:
                    date_value = format_date(df[date_column_name].iloc[0], input_format='%Y%m%d', output_format='%Y%m%d')
                    channel_id_value = df[channel_id_column_name].iloc[0]
                    print(f"For the file {file_name} date is {date_value} and channel id is {channel_id_value}")
                else:
                    print(f"Columns {date_column_name} or {channel_id_column_name} not found in {file_name}")
                    continue

                # Query to check if data exists in the table
                query = f"""
                SELECT COUNT(*) as count_rows
                FROM {table_id}
                WHERE date = '{date_value}' AND channel_id = '{channel_id_value}'
                """

                query_job = client.query(query)
                result = query_job.result()
                count_rows_in_table = list(result)[0].count_rows

                # Check if the number of rows in the table and the file are the same
                if count_rows_in_table != len(df):
                    # Delete rows from the table with the same date and channel id
                    delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE date = '{date_value}' AND channel_id = '{channel_id_value}'
                    """

                    delete_job = client.query(delete_query)
                    delete_job.result()

                    # Upload the file to the table
                    load_csv_to_bigquery(table_id, file_path)

                # If the number of rows is the same, no need to upload
                else:
                    print(f"Skipping upload for {file_name} as data already exists in {table_id}")

            except pd.errors.EmptyDataError:
                print(f"Error: EmptyDataError occurred while reading {file_name}. Skipping further processing.")
                continue

        else:
            print(f"Table ID is empty for {file_name}. Skipping further processing.")
