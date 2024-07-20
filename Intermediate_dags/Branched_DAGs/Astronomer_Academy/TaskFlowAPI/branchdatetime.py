from airflow.decorators import dag
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator
import pendulum

@dag(schedule=None, start_date=pendulum.datetime(2024, 5, 9), catchup=False)

def branchdatetime():

    empty_task_12 = EmptyOperator(task_id="date_in_range")
    empty_task_22 = EmptyOperator(task_id="date_outside_range")
    empty_task_32 = EmptyOperator(task_id="one_more_task")

    cond2 = BranchDateTimeOperator(
        task_id="datetime_branch",
        follow_task_ids_if_true=["date_in_range", "one_more_task"],
        follow_task_ids_if_false=["date_outside_range"],
        # target_lower=pendulum.datetime(2024, 5, 1),
        # target_upper=pendulum.datetime(2024, 5, 15),
        target_lower=pendulum.time(0, 0, 0),
        target_upper=pendulum.time(21, 0, 0)
    )

    cond2 >> [empty_task_12, empty_task_22, empty_task_32]

dag = branchdatetime()