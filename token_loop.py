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
import urllib
import json
from io import FileIO
import time
from  datetime import datetime as dt, timedelta
import logging
import time


# Create a logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# Create a file handler
handler = logging.FileHandler('my_log.log')
handler.setLevel(logging.DEBUG)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(handler)


# SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']
SCOPES = ['https://www.googleapis.com/auth/yt-analytics-monetary.readonly']


API_SERVICE_NAME = 'youtubereporting'
API_VERSION = 'v1'
CLIENT_SECRETS_FILE = 'secrets2.json'
#TOKEN_FILE='token_vdt.pickle'
#TOKEN_FILE='token_NME.pickle'
#TOKEN_FILE='token_NME.pickle'

CHANNEL_ID='UC7zbbglcdBEvi9C3W_K7DCQ'
API_KEY='AIzaSyB2i91QS779UFyywLOoQtae1UCmFJ36by4'
days_to_backfill=10
# os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'


def get_youtube_data(token):
    credentials =None
    # token.pickle stores the user's credentials from previously successful logins
    if os.path.exists(token):
        print('Loading Credentials From File...')
        with open(token, 'rb') as token:
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
            with open(token, 'wb') as f:
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


# Remove keyword arguments that are not set.
def remove_empty_kwargs(**kwargs):
  good_kwargs = {}
  if kwargs is not None:
    for key, value in kwargs.iteritems():
      if value:
        good_kwargs[key] = value
  return good_kwargs

# Call the YouTube Reporting API's jobs.list method to retrieve reporting jobs.
def list_reporting_jobs(youtube_reporting, **kwargs):
  # Only include the onBehalfOfContentOwner keyword argument if the user
  # set a value for the --content_owner argument.
  # kwargs = remove_empty_kwargs(**kwargs)

  # Retrieve the reporting jobs for the user (or content owner).
  results = youtube_reporting.jobs().list().execute()
  df_report_list=pd.DataFrame(columns=['id','jobId','startTime','createTime','downloadUrl','report_name'])
  if 'jobs' in results and results['jobs']:
    jobs = results['jobs']
    df_jobs=pd.DataFrame.from_dict(jobs,orient='columns')
    # print(df_jobs)
    df_report_list=pd.DataFrame(columns=['id','jobId','startTime','createTime','downloadUrl','report_name'])
    for i in df_jobs.index:
      # print ('Reporting job id: %s\n name: %s\n for reporting type: %s\n'
      #   % (job['id'], job['name'], job['reportTypeId']))
      df_rep=list_reports(df_jobs['id'][i],df_jobs['name'][i])
      df_report_list=pd.concat([df_report_list,df_rep],ignore_index=True)
    t=dt.today()-timedelta(days_to_backfill)
    # print(t.date())
    df_report_list['CreationDate']=pd.to_datetime(df_report_list['createTime']).dt.date
    df_report_filter=df_report_list[df_report_list['CreationDate']>=t.date()]
      
  else:
    print('No jobs found')
    return False

  return True,df_report_filter

def list_reports(job_id,name):
   
# this lists all the reports that are generated after you've created a job
# so it might take a while before you're able to get list a report
   reports_t=youtubeAnalytics.jobs().reports().list(jobId = job_id).execute()
   df_reports=pd.DataFrame.from_dict(reports_t['reports'],orient='columns')
   df_reports['report_name']=name
   return df_reports
 


def report_download(df,dir_name):
   for item in df.index:
    time.sleep(0.5)
    report_id = df['id'][item]
    job_id=df['jobId'][item]
    name=df['report_name'][item]
    # create_time = item['createTime']
    print(f'Report ID: {report_id}')
    report_url = youtubeAnalytics.jobs().reports().get(jobId = job_id, reportId = report_id).execute()['downloadUrl']
    current_time_seconds = time.time()
    current_time_microseconds = int(current_time_seconds * 1000000)
    current_time_microseconds_str = str(current_time_microseconds)

    file_name=dir_name+'/'+name+'_'+report_id+'_'+current_time_microseconds_str+'.csv'
    logger.info('Downloading report: %s', file_name)
    print(file_name)
    request = youtubeAnalytics.media().download(resourceName = " ")
    request.uri = report_url
    fh = FileIO(file_name, mode='wb')
    downloader = MediaIoBaseDownload(fh, request, chunksize=-1)
    done = False
    while done is False:
      status, done = downloader.next_chunk()
      if status:
        print('Download %d%%.' % int(status.progress() * 100))
        print('Download Complete!')
        logger.info('Download Complete!')




if __name__ == '__main__':
#   Disable OAuthlib's HTTPs verification when running locally.
#   *DO NOT* leave this option enabled when running in production.
  os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
# Below method call is to authenticate youtube reporting API
  token_list = [
   'token_vedantu_master_tamil_8_&_9.pickle',
   'token_vdt.pickle',
    'token_v_tamil_ncert_11-12_.pickle',
    'token_vedantu_plus_one_&_plus_two_malayalam.pickle',
    'token_neet_shala.pickle',
   'token_vedantu_9and10.pickle',
    'token_vedantu_foundation.pickle',
   'token_jee_enthuse_english.pickle',
 'token_vedantu_telugu_neet.pickle',
 'token_vedantu_young_wonders.pickle',
 'token_v_tamil_ncert_9_-_10.pickle',
 'token_vedantu_jee.pickle',
'token_vbiotonic.pickle',
 'token_vedantu_olympiad_school.pickle',
'token_vedantu_jme.pickle',
 'token_vedantu_telugu_jee.pickle',
'token_vedantu_neet_tamil.pickle',
 'token_NME.pickle',
'token_vedantu_9_&_10_maths.pickle',
 'token_vedantu_neet_english.pickle',
 'token_vedantu_telugu.pickle',
 'token_vedantu_ncert_11and12.pickle',
 'token_vedantu_class9.pickle'
'token_vedantu_neet_toppers.pickle'
    
]
  for token in token_list:
     dir_name = token.replace('.pickle', '')  # Remove the '.pickle' extension from the directory name
     if not os.path.exists(dir_name):
        # If it doesn't exist, create it
        os.mkdir(dir_name)
        logger.info('Created directory: %s', dir_name)
     logger.info('Created directory: %s', dir_name)
     
     youtubeAnalytics = get_youtube_data(token )
     # Below method call for report list for given number of days
     status, df_report_filter=list_reporting_jobs(youtubeAnalytics)
     # Method to download the report in 
     report_download(df_report_filter,dir_name)
  # df_report_filter=df_report_filter.to_csv('reports_to_download.csv')





