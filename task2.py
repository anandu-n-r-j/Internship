from datetime import datetime, timedelta
from googleapiclient.discovery import build
import pandas as pd
# Setting up API key
API_KEY = 'AIzaSyDEqAT3lFP6kJVBKgIZ0oLUJFKZgMiwePM'
def get_channel_videos(channel_id, days_back=7):
    # Calculating the date n days ago
    published_after_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Creating a YouTube API client
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    videos_info = []

    next_page_token = None
    while True:
        # Calling the search.list method to retrieve videos from the channel published in the last n days
        search_response = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            type='video',
            maxResults=50,  # Max results per page
            publishedAfter=published_after_date,
            pageToken=next_page_token
        ).execute()

        # Iterating over the search results
        for item in search_response['items']:
            video_id = item['id']['videoId']
            snippet = item['snippet']

            # Geting video details using the videos.list method
            video_response = youtube.videos().list(
                part='snippet',
                id=video_id
            ).execute()

            video_snippet = video_response['items'][0]['snippet']

            # Extracting description
            description = video_snippet['description']

            # Extracting hashtags from description
            hashtags = [tag.strip('#') for tag in description.split() if tag.startswith('#')]

            # Geting comments for the video using the commentThreads.list method
            comments_response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=200  # Limiting to 100 comments per video
            ).execute()

            # Extract comments
            comments = []
            for comment in comments_response['items']:
                comments.append(comment['snippet']['topLevelComment']['snippet']['textOriginal'])

            videos_info.append({
                'title': snippet['title'],
                'description': description,
                'hashtags': hashtags,
                'comments': comments
            })
        df =  pd.DataFrame(videos_info)
       # df['hashtags'] = df['hashtags'].apply(lambda x: '|'.join(x))
        #df['comments'] = df['comments'].apply(lambda x: '|'.join(x))
        df.to_csv('task2.csv',index=False)
        # Check if there are more pages of results
        next_page_token = search_response.get('nextPageToken')
        if not next_page_token:
            break  # No more pages, exit loop

       

    return videos_info


channel_id = 'UCi4s3kO1-xxCDaw8yX56umw'
days_back = 15  
channel_videos = get_channel_videos(channel_id, days_back)

print(f"Videos from Channel ID {channel_id} published in the last {days_back} days:")
for video in channel_videos:
    print(video)
