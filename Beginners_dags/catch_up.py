from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

def choose_branch():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='taskChoose'):
        return 'taskC'
    else:
        return 'taskD'

def task_c():
    print("TASK C executed!")

with DAG(
    #dag ID
    'cron_catchup',
    start_date=datetime(2024, 7, 14), 
    description='using crons, catchup, and backfill',
    tags=['catch_up', 'python_operator', 'BashOperator'],
    schedule='0 0 * * *', # runs at midnight. SAME AS @daily
    catchup=True,
    default_args={
        'owner': 'egi'
    }
):
    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )
    
    taskChoose = PythonOperator(
        task_id='taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable= branch
    )

    taskC = PythonOperator(
        task_id= 'taskC',
        python_callable= task_c
    )
    
    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )

taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD