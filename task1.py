from googleapiclient.discovery import build
import pandas as pd
# Settin up your API key
API_KEY = 'AIzaSyDEqAT3lFP6kJVBKgIZ0oLUJFKZgMiwePM'

# Creating a YouTube API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

def get_video_info(keyword, video_type='video', max_results=5):
    # Creating a YouTube API client
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    videos_info = []

    # Calling the search.list method to retrieve search results
    search_response = youtube.search().list(
        q=keyword,
        part='snippet',
        type=video_type,
        maxResults=max_results
    ).execute()

    # Iterating over the search results
    for item in search_response['items']:
        video_id = item['id']['videoId']
        snippet = item['snippet']

        # Getting video details using the videos.list method
        video_response = youtube.videos().list(
            part='snippet',
            id=video_id
        ).execute()

        video_snippet = video_response['items'][0]['snippet']

        # Extracting description
        description = video_snippet['description']

        # Extracting hashtags from description
        hashtags = [tag.strip('#') for tag in description.split() if tag.startswith('#')]

        # Getting comments for the video using the commentThreads.list method
        comments_response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100  # Limit to 100 comments per video
        ).execute()

        # Extracting comments
        comments = []
        for comment in comments_response['items']:
            comments.append(comment['snippet']['topLevelComment']['snippet']['textOriginal'])

        videos_info.append({
            'title': snippet['title'],
            'description': description,
            'hashtags': hashtags,
            'comments': comments
        })
        df = pd.DataFrame(videos_info)
        df.to_csv('task1.csv', index=False)

    return videos_info


keyword = 'kerala'
top_videos = get_video_info(keyword)
top_shorts = get_video_info(keyword, video_type='short')

print("Top 5 Normal Videos:")
for video in top_videos:
    print(video)

print("\nTop 5 Shorts:")
for short in top_shorts:
    print(short)
