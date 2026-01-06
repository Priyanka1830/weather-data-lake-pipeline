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

def extract_weather_data():
    # get the weather data from openweathermap api
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info("Data extraction successful.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting data: {e}")
        raise

def validate_data(data):
    # perform data validations
    if not data:
        raise ValueError("No data received from API.")

    if 'main' not in data or 'temp' not in data['main']:
        raise ValueError("Missing critical field: 'temp'")
    
    humidity = data['main'].get('humidity')
    if not (0 <= humidity <= 100):
        raise ValueError(f"Data Quality Failure: Humidity {humidity}% is out of range (0-100).")

    logging.info("Data validation passed.")
    return True

def transform_data(raw_data):
    # add metadata and temperature conversion
    weather_entry = {
        "city": raw_data["name"],
        "temperature_celsius": round(raw_data["main"]["temp"] - 273.15, 2), # Kelvin to Celsius
        "humidity": raw_data["main"]["humidity"],
        "weather_condition": raw_data["weather"][0]["description"],
        "ingestion_timestamp": datetime.now()
    }
    
    df = pd.DataFrame([weather_entry])
    logging.info("Data transformation complete.")
    return df

def load_to_s3(df):
    # Upload the parquet to S3 path
    try:
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


def run_etl_pipeline():
    # Wrapper function for Airflow PythonOperator
    logging.info("Starting ETL Pipeline...")
    raw_data = extract_weather_data()

    validate_data(raw_data)
    transformed_df = transform_data(raw_data)

    load_to_s3(transformed_df)
    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    run_etl_pipeline()