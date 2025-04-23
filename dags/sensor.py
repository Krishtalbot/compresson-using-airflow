from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor

from datetime import datetime

with DAG("dag_sensor", start_time=datetime(2025,4,22), schedule_interval="@daily",
         default_args = default_args, catchup=False) as dag:
    
    waiting_for_file = FileSensor(
        task_id = "waiting_for_file",
        poke_interval=30,
        timeout = 60*5,
        mode='reschedule',
        soft_fail=True
    )