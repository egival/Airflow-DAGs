from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator

def greeting(name):
    print(f'Hello, my name is {name}')

def greeting_with_city(name, city):
    print(f'Hello, my name is {name} from {city}')

with DAG (
    dag_id = 'PyOp_PassingParam',
    description = 'passing parameters to Python callables',
    start_date= datetime(2024, 3, 6),
    schedule = timedelta(days=1), #schedules the DAG to run every day
    catchup= False,
    default_args={
        'owner': 'egi'},
    tags = ['python', 'operator', 'parameter']
    ):    

    task1 = PythonOperator(
        task_id = 'greeting',
        python_callable= greeting,
        op_args= ['John']
    )

    task2 = PythonOperator(
        task_id= 'hello_with_city',
        python_callable= greeting_with_city,
        op_args= ['Rasa', 'Vilnius']
    )

    task1 >> task2 