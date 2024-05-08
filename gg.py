import os
import re
from datetime import timedelta
from googleapiclient.discovery import build

api_key = 'AIzaSyB2i91QS779UFyywLOoQtae1UCmFJ36by4'

youtube = build('youtube', 'v3', developerKey=api_key)

hours_pattern = re.compile(r'(\d+)H')
minutes_pattern = re.compile(r'(\d+)M')
seconds_pattern = re.compile(r'(\d+)S')

total_seconds = 0

video_id = 'zPonyyKNtZ4'  # Replace with the desired video ID

vid_request = youtube.videos().list(
    part="contentDetails",
    id=video_id
)

vid_response = vid_request.execute()

for item in vid_response['items']:
    duration = item['contentDetails']['duration']

    hours = hours_pattern.search(duration)
    minutes = minutes_pattern.search(duration)
    seconds = seconds_pattern.search(duration)

    hours = int(hours.group(1)) if hours else 0
    minutes = int(minutes.group(1)) if minutes else 0
    seconds = int(seconds.group(1)) if seconds else 0

    video_seconds = timedelta(
        hours=hours,
        minutes=minutes,
        seconds=seconds
    ).total_seconds()

    total_seconds += video_seconds

total_seconds = int(total_seconds)

minutes, seconds = divmod(total_seconds, 60)
hours, minutes = divmod(minutes, 60)

print(f'Total duration for video {video_id}: {hours}:{minutes}:{seconds}')
