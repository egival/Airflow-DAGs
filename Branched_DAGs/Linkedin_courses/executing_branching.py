from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

#task 1
def has_driving_license():
    return choice([True, False])

#task 2. If task1 is TRUE, then f-n will return 'eligible_to_drive', else 'not_eligible_to_drive'
def branch(ti):
    if ti.xcom_pull(task_ids = 'has_driving_license'):
        return 'eligible_to_drive'
        #grazina task_id pavadinima
    else:
        return 'not_eligible_to_drive'

def eligible_to_drive():
    print("You can drive, you have license!")

def not_eligible_to_drive():
    print("You need a license to drive")

with DAG(
    dag_id= 'executing_branching',
    start_date = datetime(2024, 4, 2),
    description='Running branching pipelines',
    tags=['branching', 'conditions'],
    schedule='@once',
    catchup=False,
    default_args={
        'owner': 'egi'
    }
):
    task1 = PythonOperator (
        task_id = 'has_driving_license',
        python_callable = has_driving_license
    )

    task2 = BranchPythonOperator(
        task_id = 'branch',
        python_callable= branch
    )

    task3 = PythonOperator(
        task_id = 'eligible_to_drive',
        python_callable=eligible_to_drive
    )

    task4 = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable= not_eligible_to_drive
    )

    task1 >> task2 >> [task3, task4]