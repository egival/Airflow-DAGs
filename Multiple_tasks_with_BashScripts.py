from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG (
    dag_id = 'Executing_multiple_tasks_with_bashScripts',
    description = 'DAG with multiple tasks and dependencies based on bash scripts',
    start_date=datetime(2024, 1, 1),
    schedule = timedelta(days=1), #schedule on a daily basis
    catchup= False,
    default_args={
        'owner': 'egi'
    },
    tags = ['bash', 'multiple', 'bashScripts'],
    template_searchpath = '/home/egidija1/airflow/dags/bash_scripts'
    #we need to specify the property template search path. Airflow works with Jinja templates that you can use to render(atvaizduoti) my output.
#template search path tells the Airflow DAG this is were you have to look for Jinja templates. 
#Even though here we are using bash scripts and the Jinja templates, the template search path property is what we have to specify to the DAG and point it to the folder where it should search for the scripts we are interested in. 
):
    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'taskA.sh'
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'taskB.sh'
    )

    taskC = BashOperator (
        task_id = 'taskC',
        bash_command = 'taskC.sh'
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'taskD.sh'
    )

    taskE = BashOperator(
        task_id = 'taskE',
        bash_command = 'taskE.sh'
    )
    taskF = BashOperator(
        task_id = 'taskF',
        bash_command = 'taskF.sh'
    )
    taskG = BashOperator(
        task_id = 'taskG',
        bash_command = 'taskG.sh'
    )

    taskA >> taskB >> taskE
    taskA >> taskC >> taskF
    taskA >> taskD >> taskG
