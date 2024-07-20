from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def increment_by_1(value):
    print(f"Value {value}!")
    return value +1

def multiply_by_100(ti):
    value=ti.xcom_pull(task_ids='increment_by_1')
    print(f"Value {value}!")
    return value * 100 

def subtract_9(ti):
    value = ti.xcom_pull(task_ids = 'multiply_by_100')
    print(f"Value {value}!")
    return value - 9

def print_value(ti):
    value = ti.xcom_pull(task_ids = 'subtract_9')
    print(f"Value {value}!")

with DAG(
    #dag ID
    'Xcom_PyOp_PullMeth2',
    start_date=datetime(2024, 3, 18),
    description='Special xcoms method pull and push',
    tags=['python_operator', 'xcoms'],
    schedule='@daily', #everyday at midnight
    catchup=False,
    default_args={
        'owner':'egi'
    }
):

    task1 = PythonOperator (
        task_id = "increment_by_1",
        python_callable = increment_by_1,
        op_kwargs = {'value': 1}
    )
    
    task2 = PythonOperator(
        task_id = "multiply_by_100",
        python_callable= multiply_by_100
    )

    task3 = PythonOperator(
        task_id = "subtract_9",
        python_callable= subtract_9
    )

    task4 = PythonOperator(
        task_id = "print_value",
        python_callable= print_value
    )

    task1 >> task2 >> task3 >> task4