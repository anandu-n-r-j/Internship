import os
from googleapiclient.discovery import build
from google.cloud import bigquery
from datetime import datetime, timedelta

# Set your Google Cloud project ID
project_id = 'ga4-bq-connector'

# Set your BigQuery dataset and table names
source_dataset_name = 'Vedantu_Prod'
source_table_name = 'p_YT_Data_with_Pub'
target_dataset_name = 'DimTables'
target_table_name = 'published_date_duration'

# Set your YouTube API key
api_key = 'AIzaSyB2i91QS779UFyywLOoQtae1UCmFJ36by4'

# Set the path to your service account key JSON file
credentials_path = r"C:\Users\anand\Youtube\service-account-key.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Set up the YouTube API service
youtube = build('youtube', 'v3', developerKey=api_key)

# Set up the BigQuery client
client = bigquery.Client(project=project_id)

# Calculate the date 7 days ago from today
days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
print(days_ago)
# Query BigQuery to get video IDs with activity in the last 7 days
query = f"""
    SELECT distinct video_id
    FROM `{project_id}.{source_dataset_name}.{source_table_name}`
    WHERE activity_date >= '{days_ago}' limit 50
"""
print(query)
query_job = client.query(query)
print(f'The query job is {query_job}')
video_ids = [row['video_id'] for row in query_job]
print(f'The length of the video_ id is{len(video_ids)}')
# Fetch details for each video using the YouTube Data API
results = []

for video_id in video_ids:
    request = youtube.videos().list(
        part='contentDetails,snippet',
        id=video_id
    )
    response = request.execute()

    # Check if 'items' list is not empty
    if 'items' in response and response['items']:
        # Get the duration, publish date, and title
        duration = response['items'][0]['contentDetails']['duration']
        publish_date = response['items'][0]['snippet']['publishedAt']
        title = response['items'][0]['snippet']['title']


        results.append({
            'video_id': video_id,
            'title': title,
            'duration': duration,
            'publish_date': publish_date
        })
    else:
        print(f"No data found for video ID: {video_id}")

# Create a new BigQuery table to store the results
schema = [
    bigquery.SchemaField('video_id', 'STRING'),
    bigquery.SchemaField('title', 'STRING'),
    bigquery.SchemaField('duration', 'STRING'),
    bigquery.SchemaField('publish_date', 'STRING'),
]

table_ref = client.dataset(target_dataset_name).table(target_table_name)
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)

# Insert the results into the new table
if results:
    rows_to_insert = [(result['video_id'], result['title'], result['duration'], result['publish_date']) for result in results]
    errors = client.insert_rows(table, rows_to_insert)

    if not errors:
        print(f"Data inserted into {project_id}.{target_dataset_name}.{target_table_name} successfully.")
    else:
        print(f"Error inserting data: {errors}")
