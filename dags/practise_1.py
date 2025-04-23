from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

start = DummyOperator(task_id='start')

def extract_weather():
    print('Extracting weather data...')

def transform_weather():
    print('Transforming weather data...')

def load_weather():
    print('Loading weather data...')

with DAG("Weather_ETL_pipeline", start_date=datetime(2025, 4, 22, 6, 0), 
         schedule_interval='@daily', description='Daily Weather ETL Pipeline', catchup=False) as dag:
    
    extract_weather_data = PythonOperator(
        task_id = 'extract_weather_data',
        python_callable = extract_weather
    )

    transform_weather_data = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable = transform_weather
    )

    load_weather_data = PythonOperator(
        task_id = 'load_weather_data',
        python_callable = load_weather,
        retries = 2,
        retry_delay=timedelta(minutes=1)
    )

    start >> extract_weather_data >> transform_weather_data >> load_weather_data