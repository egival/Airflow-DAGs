from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator

def print_function():
    print('Printing with Python Operator')

with DAG (
    dag_id = 'Printing_with_PythonOperator',
    description = 'DAG which prints using Python Operator',
    start_date= datetime(2024, 3, 6),
    schedule = timedelta(days=1), #schedule on a daily basis
    catchup=False,
    default_args={
        'owner': 'egi'},
    tags = ['python', 'operator', 'printing']
    ):    

    task = PythonOperator(
        task_id = 'PythonOperator',
        python_callable=print_function
    )