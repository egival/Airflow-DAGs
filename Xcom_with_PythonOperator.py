from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator

def increment_by_1(counter):
    print(f"Count {counter}!")

    return counter + 1 #value which will go to XCOMS

def multiply_by_100(counter):
    print(f"count {counter}!")

    return counter * 100 #value which will go to XCOMS

with DAG (
    dag_id = 'Xcoms_with_PythonOperator',
    description = 'Cross-task communication with Xcom',
    start_date= datetime(2024, 3, 6),
    schedule = '@daily', #as soon as unpause dag in UI, the DAG will start to run
    default_args={'owner': 'egi'},
    catchup= False,
    tags = ['python', 'operator', 'xcoms', "LinkedInLearnings"]
    ):    

    task1 = PythonOperator (
        task_id = 'increment_by_1',
        python_callable = increment_by_1,
        op_kwargs = {'counter': 100}
    )

    task2 = PythonOperator (
        task_id = 'multiply_by_100',
        python_callable =  multiply_by_100,
        op_kwargs= {'counter': 9}
    )

    task1 >> task2
