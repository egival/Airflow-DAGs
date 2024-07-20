import time
from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag (dag_id = 'XCOM_Taskflow',
    start_date=datetime(2024, 7, 6),
    description='XCom using TaskFlow API',
    tags=['xcom', 'taskflow'],
    schedule='@once',
    catchup=False,
    default_args={
        'owner': 'egi'
    })
def passing_data_with_taskflow_api():

    @task
    def get_order_prices():
        order_price_data= {
        'o1': 234.45,
        'o2': 10.00,
        'o3': 34.77,
        'o4': 45.66,
        'o5': 399
        }

        return order_price_data 
        #this return value will be added to XCOM backend
        #and this return value will be available to another task

    @task
    def compute_sum(order_price_data: dict):
        total = 0 
        for order in order_price_data:
            total += order_price_data[order]
        
        return total

    @task
    def compute_average(order_price_data: dict):

        total = 0
        count = 0
        for order in order_price_data:
            total += order_price_data[order]
            count += 1

        average = total / count

        return average

    @task
    def display_result(total, average):
        print(f"Total price of goods {total}.")
        print(f"Average price of goods {average}.")

    order_price_data = get_order_prices()

    total = compute_sum(order_price_data)
    average = compute_average(order_price_data)

    display_result(total, average)

passing_data_with_taskflow_api()
