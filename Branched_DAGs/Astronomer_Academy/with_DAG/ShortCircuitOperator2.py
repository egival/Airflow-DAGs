from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def my_func():
    import requests
    resp = requests.get('https://www.httpbin.org/status/100%2C200%2C300%2C400%2C500')
    if resp.status_code ==300:
        print(resp.status_code)
        return False
    else:
        print(resp.status_code)
        return True

with DAG ('ShortCircuitOperator2',  start_date=datetime(2024, 5, 14), schedule='@daily', catchup=False):

    cond_true = ShortCircuitOperator (
        task_id="condition_is_True",
        python_callable=my_func,
    )

    next_task = EmptyOperator(task_id="next_task")
    cond_true >> next_task


