from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from random import randint
from datetime import datetime


def _check_temperature():
    return randint(-10, 45)

def _decide_alert(ti):
    temp = ti.xcom_pull(task_id = 'check_temperature')
    if(temp>35):
        return 'heat_alert'
    elif(temp<5):
        return 'cold_alert'
    return 'no_alert'

def _heat_alert():
    print("Heat alert triggered!")

def _cold_alert():
    print("Cold alert triggered!")

def _no_alert():
    print("No alert triggered!")


with DAG("Temperature_alert", start_date=datetime(2025,4,22,7,0),
         schedule_interval='@daily', description="Temperature-Based Alert System", catchup=False) as dag:
    check_temperature = PythonOperator(
        task_id = 'check_temperature',
        python_callable = _check_temperature
    )

    decide_alert = BranchPythonOperator(
        task_id = 'decide_alert',
        python_callable = _decide_alert
    )

    heat_alert = PythonOperator(
        task_id = 'heat_alert',
        python_callable = _heat_alert
    )

    cold_alert = PythonOperator(
        task_id = 'cold_alert',
        python_callable = _cold_alert
    )

    no_alert = PythonOperator(
        task_id = 'no_alert',
        python_callable = _no_alert
    )

    end = DummyOperator(task_id="end")


    check_temperature >> decide_alert
    decide_alert >> heat_alert >> end
    decide_alert >> cold_alert >> end
    decide_alert >> no_alert >> end
