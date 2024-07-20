from airflow import DAG
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.weekday import WeekDay
import pendulum

with DAG('branchDayOfWeek2', start_date=pendulum.datetime(2024, 5, 14), schedule='@daily', catchup=False):
    
   branch_weekend=BranchDayOfWeekOperator(
    task_id = 'weekday_branch',
    follow_task_ids_if_true=["saturday_or_sunday", "one_more_task"],
    follow_task_ids_if_false=["not_saturday_or_sunday"],
    week_day = {WeekDay.SATURDAY, WeekDay.SUNDAY},
   )

   empty_task_12 = EmptyOperator(task_id="saturday_or_sunday")
   empty_task_22 = EmptyOperator(task_id="not_saturday_or_sunday")
   empty_task_32 = EmptyOperator(task_id="one_more_task")


   branch_weekend >> [empty_task_12, empty_task_22, empty_task_32]