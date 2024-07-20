from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import random
import time

def task1():
    print("I am printing with Python operator")

def task2():
    rand_num = random.randint(1, 10)
    print(f"I am printing random number from 1 to 10: {rand_num}")

def task3():
    time.sleep(5)
    print("Task is executed!")

with DAG(
    #dag ID
    'Multiple_PythonOp_prints',
    start_date=datetime(2024, 3, 17),
    description='A simple DAG for printing',
    tags=['python_operator', 'simple'],
    schedule='@daily', #everyday at midnight
    catchup=False,
    default_args={
        'owner':'egi'
    }
):

    Task1 = PythonOperator(
    task_id = 'python_Op_print1',
    python_callable=task1
    )

    Task2 = PythonOperator(
    task_id = 'python_Op_print2',
    python_callable=task2
    )

    Task3 = PythonOperator(
    task_id = 'python_Op_print3',
    python_callable=task3
    )

    Task1 >>[Task2, Task3]