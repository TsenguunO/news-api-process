import json
import boto3
from datetime import datetime, timedelta
import os
from newsapi import NewsApiClient

# Initialize NewsApiClient
newsapi = NewsApiClient(api_key='67e412776a004573bd90fc044f179bd4')

# Initialize boto3 client for S3
s3 = boto3.client('s3', region_name='ap-southeast-2')

def lambda_handler(event, context):
    # Parameters from the event (you can set default values if not provided in the event)
    from_date = event.get('from_date', '2025-03-01')
    to_date = event.get('to_date', '2025-03-05')
    topic = 'Australia'
    language = 'en'
    bucket_name = 'news-api-bucket-sek'

    # Convert from_date and to_date to datetime objects
    start_date = datetime.strptime(from_date, '%Y-%m-%d')
    end_date = datetime.strptime(to_date, '%Y-%m-%d')

    # Calculate the difference in days
    days_diff = (end_date - start_date).days

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

        # Save articles to a JSON file in the temporary directory
        file_name = f'/tmp/news_{date_str}.json'
        with open(file_name, 'w') as f:
            json.dump(all_articles, f)

        # Create the prefix using year/month/day format
        prefix = f"{date.year}/{date.month:02d}/"

        # Upload the file to S3 with the prefix (this will create the structure year/month/day/ in the S3 bucket)
        s3.upload_file(file_name, bucket_name, f'{prefix}{file_name.split("/")[-1]}')
        print(f"File uploaded to: {prefix}{file_name.split('/')[-1]}")

        # Optional: Delete the local file after uploading (it’s stored in the /tmp directory, which is Lambda’s temporary storage)
        os.remove(file_name)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }
