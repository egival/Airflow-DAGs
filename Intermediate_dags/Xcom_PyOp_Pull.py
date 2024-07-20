#DAG to share date (string 'iphone') between the tasks
#When you return a value from the Python operator, that value becomes automatically xcom
#with the key return_value, so you can share it with other tasks.

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def peter_task(ti=None):
#ti - task instance object; it calls a special method that allows to push XCOM
#we want to share the string 'iphone' with another task
    return 'iphone'

def bryan_task(ti):
#this function expects to get a mobile phone
    mobile_phone=ti.xcom_pull(task_ids="peter_task", key="return_value")
    print(f"My mobile phone is {mobile_phone}")

with DAG (
    #dag ID
    'Xcoms_Pullmethod',
    tags=['data engineering'],
    start_date=datetime(2024, 1, 1),
    schedule = '@daily',
    catchup = False,
    default_args={'owner': 'egi'}
):
    task1=PythonOperator(task_id='peter_task', python_callable=peter_task)
    task2=PythonOperator(task_id='bryan_task', python_callable=bryan_task)
    task1 >> task2