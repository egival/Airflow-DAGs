from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime
import pendulum

def my_func():
    import requests
    resp = requests.get('https://www.httpbin.org/status/100%2C200%2C300%2C400%2C500')
    if resp.status_code ==500:
        print(resp.status_code)
        return False
    else:
        print(resp.status_code)
        return True

@dag(schedule_interval=None, 
start_date=pendulum.datetime(2024, 5, 1),
schedule="@daily",
catchup=False
)
def short_circuit():
    cond_true = ShortCircuitOperator (
        task_id="condition_is_True",
        python_callable=my_func,
    )

    next_task = EmptyOperator(task_id="next_task")
    cond_true >> next_task
    
short_circuit()
# dag = short_circuit()


