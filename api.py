from newsapi import NewsApiClient
import pandas as pd
import json
import boto3
from datetime import datetime, timedelta
import os
# Init NewsApiClient
newsapi = NewsApiClient(api_key='67e412776a004573bd90fc044f179bd4')

# Parameters
to_date = '2025-03-05'
from_date = '2025-03-01'
topic = 'Australia'
country = 'au'
language = 'en'
bucket_name = 'news-api-bucket-sek'

# Convert from_date and to_date to datetime objects
start_date = datetime.strptime(from_date, '%Y-%m-%d')
end_date = datetime.strptime(to_date, '%Y-%m-%d')

# Calculate the difference in days
days_diff = (end_date - start_date).days

# Initialize boto3 client for S3
s3 = boto3.client('s3', region_name='ap-southeast-2')

# Loop through each day in the date range
for i in range(days_diff + 1):
    date = start_date + timedelta(days=i)
    date_str = date.strftime('%Y-%m-%d')
    print(f"Fetching articles for {date_str}")

    # Fetch articles from NewsAPI
    all_articles = newsapi.get_everything(
        q=topic,
        from_param=date_str,
        to=date_str,
        language=language,
    )

    # Save articles to a JSON file
    file_name = f'news_{date_str}.json'
    with open(file_name, 'w') as f:
        json.dump(all_articles, f)

    # Create the prefix using year/month/day format
    prefix = f"{date.year}/{date.month:02d}/"

    # Upload the file to S3 with the prefix (this will create the structure year/month/day/ in the S3 bucket)
    s3.upload_file(file_name, bucket_name, f'{prefix}{file_name}')
    print(f"File uploaded to: {prefix}{file_name}")

    # Optional: Delete the local file after uploading
    os.remove(file_name)
