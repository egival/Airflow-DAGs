from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

#define function to get xcom value
def val_for_branch():
    return 3 #supushinamas i Xcomus. Pasitikrinti AIrflow UI
  
#define branching function
def branch_func(ti):
    xcom_value = ti.xcom_pull(task_ids='val_for_branch')
    if xcom_value >= 5:
        return "continue_task"
    elif xcom_value >= 3:
        return "stop_task"
    else:
        return None 

with DAG('branchingPythonOp2', start_date=datetime(2024, 5, 14), schedule='@daily', catchup=False):
    #upstream task
    get_value = PythonOperator(
        task_id='val_for_branch',
        python_callable = val_for_branch,
    )

    branch_task = BranchPythonOperator(
        task_id = 'branch_task',
        python_callable=branch_func,
    )

    #downstream task
    continue_op = EmptyOperator(task_id="continue_task")
    stop_op=EmptyOperator(task_id="stop_task")

    #set dependencies
    get_value >> branch_task >> [continue_op, stop_op]