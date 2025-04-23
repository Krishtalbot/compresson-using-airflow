from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime

with DAG("test_dag", start_date=datetime(2025,4,22), 
         schedule='@daily', description="Training ML models", 
         tags=["First dag", "Cool"], catchup=False) as dag:
    
    @task
    def training_model(accuracy):
        return accuracy

    
    @task.branch
    def best_model(accuracy):
        if (max(accuracy)>8):
            return 'accurate'
        return 'inaccurate'

    accurate = BashOperator(
        task_id = "accurate",
        bash_command = "echo 'accurate'"
    )
    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command = "echo 'inaccurate'"
    )

    best_model(training_model.expand(accuracy=[5, 10, 7])) >> [accurate, inaccurate]

