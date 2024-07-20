from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pendulum

@dag (schedule=None, start_date=pendulum.datetime(2024, 5, 14), catchup=False)

# condition
def branch_python():
    
    @task.branch(task_id="branch_task")
    def branch_func(xcom_value):
        if xcom_value >= 5:
            return "continue_task"
        elif xcom_value >= 3:
            return "stop_task"
        else:
            return None

#upstream task
    @task
    def val_for_branch():
        return 6

#downstream task
    continue_op = EmptyOperator(task_id="continue_task")
    stop_op=EmptyOperator(task_id="stop_task")

#creating dependencies
    branch_op = branch_func(val_for_branch())
    branch_op >> [continue_op, stop_op]

dag = branch_python()