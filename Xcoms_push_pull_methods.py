#DAG to share date (string 'iphone') between the tasks
#if you want to define a specific key to xcom and pull an XCOM with a specific key
#Let’s say that we want to share string ‘iphone’ by creating XCOM with that value in it and the key: mobile_phone.

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def peter_task(ti=None):
#ti - task instance object; it calls a special method that allos to push XCOM
    ti.xcom_push(key='mobile_phone', value='iphone') #specifying xcoms key and value that I want to share with other tasks

#this function expects to get a mobile phone info
def bryan_task(ti=None):
    phone=ti.xcom_pull(task_ids='peter_task', key='mobile_phone')
    print(phone)

with DAG (
    #dag ID
    'xcoms_push_pull_methods',
    tags=['data engineering'],
    start_date=datetime(2024, 1, 1),
    schedule = '@daily',
    catchup = False,
    default_args={
        'owner': 'egi'}
):
    task1=PythonOperator(task_id='peter_task', python_callable=peter_task)
    task2=PythonOperator(task_id='bryan_task', python_callable=bryan_task)
    task1 >> task2