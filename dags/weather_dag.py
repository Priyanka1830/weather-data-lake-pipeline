from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl_weather import extract_weather_data, transform_weather_data, load_weather_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    'weather_data_lake_pipeline',
    default_args=default_args,
    description='Weather Data Pipeline: Extract -> Transform -> Load',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # node 1: Extract
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_weather_data,
        provide_context=True  # this passes the task instance (ti) to the function
    )

    # node 2: Transform
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_weather_data,
        provide_context=True
    )

    # node 3: Load
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_weather_to_s3,
        provide_context=True
    )

    # defining the DAG
    extract_task >> transform_task >> load_task