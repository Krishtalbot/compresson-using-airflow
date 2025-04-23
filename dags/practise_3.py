from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import random
from datetime import datetime

def _get_quote():
    arr = ["Today is great", "There is tmr", "This is cool", "Lol"]
    return random.choice(arr)

def _read_quote(ti, **kwargs):
    file_path = '/tmp/quote.txt'
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            print(f"📜 Quote from file: {content}")
            ti.xcom_push(key='file_content', value=content)
    except FileNotFoundError:
        error_msg = f"File {file_path} not found"
        print(error_msg)
        ti.xcom_push(key='file_content', value=error_msg)

with DAG("Quote", start_date=datetime(2025, 4, 22, 8, 0), schedule_interval='@daily',
         description='Quote of the Day Pipeline', catchup=False) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_quote = PythonOperator(
        task_id='get_quote',
        python_callable=_get_quote
    )

    save_quote = BashOperator(
        task_id='save_quote',
        bash_command='echo "$QUOTE" > /tmp/quote.txt',
        env={'QUOTE': '{{ ti.xcom_pull(task_id="get_quote") }}'}
    )

    read_quote = PythonOperator(
        task_id='read_quote',
        python_callable=_read_quote
    )

    start >> get_quote >> save_quote >> read_quote >> end
