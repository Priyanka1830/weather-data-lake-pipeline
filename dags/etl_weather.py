import requests
import pandas as pd
import boto3
import logging
import os
from datetime import datetime
from io import BytesIO
import pytz

timezone = pytz.timezone('Asia/Kolkata')
current_time = datetime.now(timezone).strftime('%Y%m%d%H%M')

AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION=os.getenv('AWS_DEFAULT_REGION')

API_KEY = os.getenv('OPEN_WEATHER_API_KEY')
CITY = "Bangalore"
BUCKET_NAME = "priyanka-data-engineering-project-bucket" 
S3_FILE_PATH = f"weather_data/weather_{current_time}.parquet"

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_weather_data(**kwargs):
    # get the weather data from openweathermap api
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info("Data extraction successful.")
        # push the data to Xcom
        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting data: {e}")
        raise

def transform_weather_data(**kwargs):

    # pull data from the previous task
    task_instance = kwargs['ti']
    raw_data = task_instance.xcom_pull(task_ids='extract_task')

    if not raw_data:
        raise ValueError("No data received from extract_task")

    # validation logic
    if 'main' not in raw_data or 'temp' not in raw_data['main']:
        raise ValueError("Missing critical field: 'temp'")
    
    humidity = raw_data['main'].get('humidity')
    if not (0 <= humidity <= 100):
        raise ValueError(f"Data Quality Failure: Humidity {humidity}% is out of range.")

    # add metadata and temperature conversion
    transformed_data = {
        "city": raw_data["name"],
        "temperature_celsius": round(raw_data["main"]["temp"] - 273.15, 2),
        "humidity": raw_data["main"]["humidity"],
        "weather_condition": raw_data["weather"][0]["description"],
        "ingestion_timestamp": datetime.now(timezone).isoformat() 
    }
    
    logging.info(f"Data transformation complete: {transformed_data}")
    return transformed_data

def load_weather_to_s3(**kwargs):

    task_instance = kwargs['ti']
    clean_data = task_instance.xcom_pull(task_ids='transform_task')

    if not clean_data:
        raise ValueError("No data received from transform_task")

    try:
        # create pandas dataframe
        df = pd.DataFrame([clean_data])

        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION
        )

        # convert df to Parquet buffer
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        
        # Upload
        s3_client.put_object(Bucket=BUCKET_NAME, Key=S3_FILE_PATH, Body=out_buffer.getvalue())
        logging.info(f"Successfully loaded data to s3://{BUCKET_NAME}/{S3_FILE_PATH}")
        
    except Exception as e:
        logging.error(f"Failed to upload to S3: {e}")
        raise
