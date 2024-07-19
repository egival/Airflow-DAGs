"""Access variables from DAG"""

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

#creating a function that expects ml_parameter
def _ml_task(ml_parameter):
   print(ml_parameter)

with DAG('ml_dag_with_variable', start_date=datetime(2024, 1, 1),
   schedule='@daily', catchup=False) as dag:

    ml_tasks = []


#Creating multiple of tasks according to the number of values that we have in variable 
#key="ml_model_parameters", value= {"param": [100, 150, 200]} 

    for ml_parameter in Variable.get('ml_model_parameters', deserialize_json=True)["param"]: 
        #as ml_model_parameters is in json, I need to deserialize into Python object

       ml_tasks.append(PythonOperator(task_id=f'ml_task_{ml_parameter}',
           python_callable=_ml_task,
           op_kwargs={'ml_parameter': ml_parameter}
    ))

    report = BashOperator(
        task_id = 'report',
        bash_command = 'echo "hello world. Egi"'
    )

    ml_tasks >> report