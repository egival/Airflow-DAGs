from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG (
    dag_id = 'Multiple_tasks_with_BashOperator',
    description = 'DAG with multiple tasks and dependencies',
    start_date= datetime(2024, 7, 1),
    schedule = '@once',
    default_args={
        'owner': 'egi'
    },
    tags = ['bashOperator', 'multiple']

):
    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = '''
        echo TASK A has started!
        for i in {1..10}
        do
            echo TASK A printing $i
        done

        echo TASK A has ended!
        '''
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = '''
        echo TASK B has started!
        sleep 4
        echo TASK B has ended!
        '''
    )

    taskC = BashOperator (
        task_id = 'taskC',
        bash_command = '''
        echo TASK C has started!
        sleep 15
        echo TASK C has ended!
        '''
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D completed!'
    )

    taskA >> [taskB, taskC]
    taskD << [taskB, taskC]
