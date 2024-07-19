from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='BashOperator_dag',
    description= 'Simple BashOperator dag',
    start_date=datetime(2024, 1, 12),
    schedule_interval='@daily',
    catchup=False,
    default_args={
    'owner': 'egi',
    #maximum number of retries
    'retries': 5,
    #for retry delay need to import timedelta pachage. Set 2 min for every retry
    'retry_delay': timedelta(minutes=2)
    
}):

#First task: print message "Hello World!" using Bash operator
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task"
    )
 
#Second task: prints message if 1st task is successful 
    task2=BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2 and I will be running after task1"
    )

#Third task: will be runing after task1 and at the same time as task2
    task3= BashOperator(
    task_id='third_task',
    bash_command="echo hey, I am task3 and will be running after task1 and at the same time as task2"
    )

    #Task dependency methods:
    #method 1
    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    #method 2
    #task1 >> task2
    #task1 >> task3

    #method 3
    task1 >> [task2, task3]