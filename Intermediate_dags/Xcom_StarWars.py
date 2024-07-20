from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime, timedelta

#first define functions to push xcom and to pull it
def _transform(ti):
   #jei nepaduosi ti kintamuju failinsi; jei visadfa kintamasis turi buti paduotas, tada nereikai NOne; ti=None- reiskia kintamajo padavimas yra optional dalykas. Geriau be jokiu optional.
   import requests
   resp = requests.get('https://swapi.dev/api/people/1').json()
   #requesta grazina json formatu
   print(resp)
   my_character = {
      "height" : int(resp["height"]) - 20, 
      "mass" : int(resp["mass"]) - 50,
      "hair_color" : "black" if resp["hair_color"] == "blond" else "blond",
      "eye_color" : "hazel" if resp["eye_color"] == "blue" else "blue",
      "gender" : "female" if resp["gender"] == "male" else "female"
      }  
   ti.xcom_push(key = 'character_info', value = my_character)

def _load(ti):
   print(ti.xcom_pull(task_ids = 'transform', key = 'character_info')) 

#we need to create a DAG object
with DAG (
   #dag ID
    'XcomStarWars',
    tags=['data engineering', 'Astronomer'],
    start_date=datetime(2024, 1, 1),
    schedule = '@daily',
    catchup = False,
    default_args={
        'owner': 'egi'
    }
): 
#then we need to create dependencies between 2 tasks:
   task1 = PythonOperator(task_id='transform', python_callable = _transform)
   task2 = PythonOperator(task_id='load', python_callable=_load)

   task1 >> task2