from airflow.operators.sql import BranchSQLOperator
from airflow.decorators import dag
import pendulum
from airflow.operators.empty import EmptyOperator

@dag(schedule=None, start_date=pendulum.datetime(2024, 5, 25), catchup=False)

def branchsql():
    empty_task_12 = EmptyOperator(task_id="count_at_least_one")
    empty_task_22 = EmptyOperator(task_id="count_is_zero")
    empty_task_32 = EmptyOperator(task_id="one_more_task")

    count_rows = BranchSQLOperator(
        task_id="count_rows",
        conn_id= "postgres_connection",
        sql = "SELECT count(*) FROM customers_table",
        follow_task_ids_if_true=["count_at_least_one", "one_more_task"],
        follow_task_ids_if_false="count_is_zero",
    )

    count_rows >> [empty_task_12, empty_task_22, empty_task_32]

dag=branchsql()