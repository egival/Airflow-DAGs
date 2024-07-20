#not working properly. There is error
from airflow.decorators import dag, task
from pendulum import datetime
import requests

API = "https://www.boredapi.com/api/activity?"

@dag(
    start_date=datetime(2024, 1, 9),
    schedule="@daily",
    tags=["activity"],
    catchup=False,
    default_args={
        'owner': 'egi'
    }
)
def find_activity():
    """
    This DAG retrieves JSON data from a specified API.
    """

    @task
    def get_activity():
        """
        Fetches JSON data from the API and returns it.
        """
        r = requests.get(API, timeout=5)
        return r.json()

    # Define the dependency between tasks
    get_activity()

# Run the DAG (implicit call due to decorator)
find_activity()