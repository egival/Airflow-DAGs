from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG (
    dag_id = 'hello_world',
    description = 'Our first "Hello World" DAG!',
    start_date= datetime(2024, 7, 1),
    schedule = None, #setting to None means DAG will not be scheduled at any point of time.
    #I will trigger a DAG when I need in Airflow UI. 
    default_args={
        'owner': 'egi'
    },
    tags = ['bash', 'no_schedule']

):
    task = BashOperator(
        task_id = 'Hello_world',
        bash_command = 'echo Hello World!',
    )