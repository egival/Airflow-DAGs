from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    #dag id
    dag_id='check_dag',
    description="DAG to check data",
    tags=['data engineering'],
    start_date=datetime(2023,1,1),
    schedule = '@daily', #everytime at midnight
    catchup=False,
    default_args={
        'owner':'egi'
    }
): 
    #first task creates a file "dummy" in the "egidija1" directory with "Hi there!"
    task1=BashOperator(
        task_id='create_file',
        bash_command='echo "Hi there!" >/home/egidija1/dummy.txt'
    )
    #second task task executes the following  Bash command: test -f /home/egidija1/dummy.txt, 
    #to verify that the file dummy exists in the egidija1 directory
    task2=BashOperator(
        task_id='check_file_exists',
        bash_command='test -f /home/egidija1/dummy.txt'
    )
    #The third task executes the following Python function: lambda: print(open('/home/egidija1/dummy.txt', 'rb').read())
    task3=PythonOperator(
        task_id='read_file',
        python_callable=lambda: print(open('/home/egidija1/dummy.txt', 'rb').read())
    )

    task1>>task2>>task3