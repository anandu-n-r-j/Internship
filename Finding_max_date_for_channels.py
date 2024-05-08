from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Replace with your specific project ID and dataset ID
project_id = "ga4-bq-connector"
dataset_id = "YoutubeTamil9to10"

# Set the path to your service account key JSON file
credentials_path = r"C:\Users\anand\Youtube\service-account-key.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Create a BigQuery client with explicit credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(project=project_id, credentials=credentials)

# Specify the channel ID for which you want to find the maximum date
channel_id = "UCS1L9B6Swy1WVXOMWgJuMuw"

# Define the list of tables
table_names= [
    "p_playlist_province_a1_vedTam",
    "p_channel_annotations_a1_vedTam",
    "p_channel_cards_a1_vedTam",
    "p_channel_province_a2_vedTam",
    "p_channel_sharing_service_a1_vedTam",
    "p_channel_end_screens_a1_vedTam",
    "p_playlist_basic_a1_vedTam",
    "p_playlist_playback_location_a1_vedTam",
    "p_playlist_device_os_a1_vedTam",
    "p_playlist_traffic_source_a1_vedTam",
    "p_playlist_combined_a1_vedTam",
    "p_channel_combined_a2_vedTam",
    "p_channel_demographics_a1_vedTam",
    "p_channel_subtitles_a2_vedTam",
    "p_channel_playback_location_a2_vedTam",
    "p_channel_device_os_a2_vedTam",
    "p_channel_basic_a2_vedTam",
    "p_channel_traffic_source_a2_vedTam"
]

# Iterate over each table and find the maximum date for the specified channel ID
for table_name in table_names:
    try:
        # Construct the full table ID
        table_id = f"`{project_id}.{dataset_id}.{table_name}`"

        # Execute the BigQuery query to get the maximum date for the specified channel ID
        query = f"SELECT MAX(date) AS max_date FROM {table_id} WHERE channel_id = '{channel_id}'"
        query_job = client.query(query)
        results = query_job.result()

        # Extract the maximum date
        max_date_from_bigquery = None
        for row in results:
            max_date_from_bigquery = row["max_date"]
            print(f"The maximum date for table {table_name} and channel ID {channel_id} is: {max_date_from_bigquery}")

        # Use max_date_from_bigquery as needed in your further processing

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
