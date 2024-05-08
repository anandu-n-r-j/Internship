import os
import pickle
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd

# from analytix.youtube import YoutubeAnalytics, YoutubeService 

import urllib
import json
from io import FileIO


# SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']
SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']


API_SERVICE_NAME = 'youtubereporting'
API_VERSION = 'v1'
CLIENT_SECRETS_FILE = 'secrets2.json'
TOKEN_FILE='token_vbiotonic.pickle'

#CHANNEL_ID='UC7zbbglcdBEvi9C3W_K7DCQ'
#API_KEY='AIzaSyB2i91QS779UFyywLOoQtae1UCmFJ36by4'
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'


def get_youtube_data():
    credentials =None
    # token.pickle stores the user's credentials from previously successful logins
    if os.path.exists(TOKEN_FILE):
        print('Loading Credentials From File...')
        with open(TOKEN_FILE, 'rb') as token:
            credentials = pickle.load(token)

    # If there are no valid credentials available, then either refresh the token or log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            print('Refreshing Access Token...')
            credentials.refresh(Request())
        else:
            print('Fetching New Tokens...')
            credentials= get_service()
            # Save the credentials for the next run
            with open(TOKEN_FILE, 'wb') as f:
                print('Saving Credentials for Future Use...')
                pickle.dump(credentials, f)
    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)



# print(credentials.to_json())
def get_service():
  flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
#   credentials = flow.run_console()
  flow.run_local_server(port=8080, prompt='consent',
                              authorization_prompt_message='')
  credentials = flow.credentials
#   print(credentials.to_json())
  return credentials
# build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

def execute_api_request(client_library_function, **kwargs):
    response = client_library_function(**kwargs).execute()
    # print(response)
    return response
    # df=pd.DataFrame.from_dict(response)
    # df.to_csv('vd_test.csv')




def create_jobs():
  # Provided JSON data
  json_data = '''
  {
    "reportTypes": [
      {
        "id": "channel_annotations_a1",
        "name": "Annotations"
      },
      {
        "id": "channel_basic_a2",
        "name": "User activity"
      },
      {
        "id": "channel_cards_a1",
        "name": "Cards"
      },
      {
        "id": "channel_combined_a2",
        "name": "Combined"
      },
      {
        "id": "channel_demographics_a1",
        "name": "Demographics"
      },
      {
        "id": "channel_device_os_a2",
        "name": "Device and OS"
      },
      {
        "id": "channel_end_screens_a1",
        "name": "End screens"
      },
      {
        "id": "channel_playback_location_a2",
        "name": "Playback locations"
      },
      {
        "id": "channel_province_a2",
        "name": "Province"
      },
      {
        "id": "channel_sharing_service_a1",
        "name": "Sharing service"
      },
      {
        "id": "channel_subtitles_a2",
        "name": "Subtitles"
      },
      {
        "id": "channel_traffic_source_a2",
        "name": "Traffic sources"
      },
      {
        "id": "playlist_basic_a1",
        "name": "Playlist user activity"
      },
      {
        "id": "playlist_combined_a1",
        "name": "Playlist combined"
      },
      {
        "id": "playlist_device_os_a1",
        "name": "Playlist device and OS"
      },
      {
        "id": "playlist_playback_location_a1",
        "name": "Playlist playback locations"
      },
      {
        "id": "playlist_province_a1",
        "name": "Playlist province"
      },
      {
        "id": "playlist_traffic_source_a1",
        "name": "Playlist traffic sources"
      }
    ]
  }'''

    # Load JSON data
  data = json.loads(json_data)
  i=0
  # Extract id and name
  for report_type in data["reportTypes"]:
      print("Report Type Obj  ",report_type)
      report_id = report_type["id"]
      name = report_type["name"]
      print(f"ID: {report_id}, Name: {name}")
      i=i+1
      # print(i)
      youtubeAnalytics.jobs().create(body={'reportTypeId':report_id, 'name': name}).execute()
      print(i)

if __name__ == '__main__':
#   Disable OAuthlib's HTTPs verification when running locally.
#   *DO NOT* leave this option enabled when running in production.
  os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

  youtubeAnalytics = get_youtube_data()
  create_jobs()
  results = youtubeAnalytics.jobs().list().execute()
  print(results['jobs'])



    # print(f'Job created ID: {report_id}  and name::{name}')
#   print(youtubeAnalytics.reportTypes().list().execute())
#   results = youtubeAnalytics.jobs().list().execute()
#   print(results['jobs'])
#   job_id='78847262-c2db-430e-8055-eca9684968e1'

#   list_reporting_jobs(youtubeAnalytics,
#                              onBehalfOfContentOwner='UC7zbbglcdBEvi9C3W_K7DCQ')

# this lists all the reports that are generated after you've created a job
# so it might take a while before you're able to get list a report
#   job_id='LG_Test'
#   reports_t=youtubeAnalytics.jobs().reports().list(jobId = job_id).execute()
#   print(reports_t)
#   request = youtubeAnalytics.media().download(resourceName = " ")
#   this uses a reportId from one of the reports that are listed above
#   report_url = youtubeAnalytics.jobs().reports().get(jobId = job_id, reportId = '9670451458').execute()['downloadUrl']
#   request = youtubeAnalytics.media().download(resourceName = " ")
#   request.uri = report_url
#   fh = FileIO('yt_test.txt', mode='wb')
#   downloader = MediaIoBaseDownload(fh, request, chunksize=-1)
#   done = False
#   while done is False:
#     status, done = downloader.next_chunk()
#     if status:
#         print('Download %d%%.' % int(status.progress() * 100))
#         print('Download Complete!')
#   list_reporting_jobs(youtubeAnalytics,
#                              onBehalfOfContentOwner='UC7zbbglcdBEvi9C3W_K7DCQ')
