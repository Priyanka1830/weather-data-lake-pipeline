from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl_weather import run_etl_pipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    'weather_data_lake_pipeline',
    default_args=default_args,
    description='Fetches weather data and saves to S3 as Parquet',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['data-engineering', 's3'],
) as dag:

    run_etl_task = PythonOperator(
        task_id='run_weather_etl',
        python_callable=run_etl_pipeline,
    )

    run_etl_task