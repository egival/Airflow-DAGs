from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG (
#dag ID
    'copy_files3',
    description="Copy files from source directory to destination directory",
    tags=['data engineering'],
    start_date=datetime(2024, 1, 1),
    schedule = '@daily',
    catchup=False,
    default_args={
        'owner': 'egi'

    }
):
#1st task create a file in directory "egidija1" with "High there!"
    task1=BashOperator(
        task_id='create_file',
        bash_command='echo "Hi there!" >/home/egidija1/hello.txt'
    )

#2nd task to copy file helo to airflow directory
    task2=BashOperator(
        task_id="copy_files",
        bash_command='cp /home/egidija1/hello.txt /home/egidija1/Copied_files/'
    )

    task1>>task2