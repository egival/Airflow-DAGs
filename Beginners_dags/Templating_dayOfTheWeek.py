from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG (
    'Day_of_the_week',
    description = "printing day of the week using templating",
    tags = ['data engineering'],
    start_date = datetime(2024, 1, 1),
    schedule= '@daily',
    catchup= False,
    default_args= {
        'owner':'egi'
    }
):
    task1 = BashOperator (
        task_id = 'print_date_of_week',
        bash_command = "echo Today is {{data_interval_start.format ('dddd')}}"
        # bash_command = "echo Today is {{ ts_nodash }}" - {{ ts_nodash }} is a shorthand for {{ execution_date.format('%Y-%m-%d') }} and provides the execution date in YYYY-MM-DD format. 
    )