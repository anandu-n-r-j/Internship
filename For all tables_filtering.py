from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pandas as pd

# Replace with your specific project ID, dataset ID, and table IDs
project_id = "ga4-bq-connector"
dataset_id = "YoutubeTamil9to10"
table_mapping = {
    "p_playlist_province_a1_vedTam": "Playlist province_combined.csv",
    "p_channel_annotations_a1_vedTam": "Annotations_combined.csv",
    "p_channel_cards_a1_vedTam": "Cards_combined.csv",
    "p_channel_province_a2_vedTam": "Province_combined.csv",
    "p_channel_sharing_service_a1_vedTam": "Sharing service_combined.csv",
    "p_channel_end_screens_a1_vedTam": "End screens_combined.csv",
    "p_playlist_basic_a1_vedTam": "Playlist user activity_combined.csv",
    "p_playlist_playback_location_a1_vedTam": "Playlist playback locations_combined.csv",
    "p_playlist_device_os_a1_vedTam": "Playlist device and OS_combined.csv",
    "p_playlist_traffic_source_a1_vedTam": "Playlist traffic sources_combined.csv",
    "p_playlist_combined_a1_vedTam": "Playlist combined_combined.csv",
    "p_channel_combined_a2_vedTam": "Combined_combined.csv",
    "p_channel_demographics_a1_vedTam": "Demographics_combined.csv",
    "p_channel_subtitles_a2_vedTam": "Subtitles_combined.csv",
    "p_channel_playback_location_a2_vedTam": "Playback locations_combined.csv",
    "p_channel_device_os_a2_vedTam": "Device and OS_combined.csv",
    "p_channel_basic_a2_vedTam": "User Activity_combined.csv",
    "p_channel_traffic_source_a2_vedTam": "Traffic sources_combined.csv"
}

# Set the path to your service account key JSON file
credentials_path = r"C:\Users\anand\Youtube\service-account-key.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Create a BigQuery client with explicit credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(project=project_id, credentials=credentials)

# Define the folder to save filtered CSV files
filtered_csv_folder = r"C:\Users\anand\OneDrive\Documents\Intern_Project\yt_reports\Filtered_csv"

# Iterate over each table and filter the corresponding CSV file
for table_name, csv_filename in table_mapping.items():
    try:
        # Construct the full table ID
        table_id = f"`{project_id}.{dataset_id}.{table_name}`"

        # Execute the BigQuery query to get the maximum date
        query = f"SELECT MAX(date) AS max_date FROM {table_id}"
        query_job = client.query(query)
        results = query_job.result()

        # Extract the maximum date
        max_date_from_bigquery = None
        for row in results:
            max_date_from_bigquery = row["max_date"]
            print(f"The maximum date for table {table_name} is: {max_date_from_bigquery}")

        if max_date_from_bigquery:
            # Load the corresponding CSV file into a DataFrame
            csv_file_path = os.path.join(
                r"C:\Users\anand\OneDrive\Documents\Intern_Project\yt_reports\combined_csv_files",
                csv_filename
            )
            df = pd.read_csv(csv_file_path)

            # Assuming the date column in the CSV is named 'date', replace it with the actual column name
            date_column_name = 'date'

            # Convert the date column to datetime type
            df[date_column_name] = pd.to_datetime(df[date_column_name], format='%Y%m%d', errors='coerce')

            # Convert the maximum date from BigQuery to datetime
            max_date_from_bigquery = pd.to_datetime(max_date_from_bigquery, format='%Y%m%d', errors='coerce')

            # Filter rows where the date is greater than the maximum date from BigQuery
            filtered_df = df[df[date_column_name] > max_date_from_bigquery]

            # Save the filtered DataFrame to a new CSV file in the specified folder
            filtered_csv_path = os.path.join(filtered_csv_folder, f"{table_name}_filtered.csv")
            filtered_df.to_csv(filtered_csv_path, index=False)
            print(f"Filtered CSV saved for table {table_name}")

            # Print the filtered rows
            print(f"Filtered rows for table {table_name}:\n{filtered_df}")

        else:
            print(f"No valid maximum date obtained from BigQuery for table {table_name}")

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
