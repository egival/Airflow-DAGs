import pandas as pd 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable

DATASETS_PATH = '/home/egidija1/airflow/insurance.csv'
OUTPUT_PATH = '/home/egidija1/airflow/output/{0}.csv'
#{0} - is a place holder. The name of output document will be saved later

def read_csv_file():
    df = pd.read_csv(DATASETS_PATH)
    print(df)
    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()

def determine_branch():
    #"transform_action" is a variable's key name
    #"filter_by_southwest" is variable's value
    transform_action = Variable.get("transform_action", default_var=None)
    #default_var=None: This argument specifies the default value to return if the variable "transform_action" doesn't exist. 
    if transform_action.startswith('filter'):
        return transform_action
    elif transform_action == 'groupby_region_smoker':
        return 'groupby_region_smoker'

    #functions performing filtering actions
def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    #column name: df['region'] == 'southwest'
    #sukuria nauja df ()

    region_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast'] #sukuriu reiksme, bet ji niekur nera issaugota.

    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False) #issaugo cia

def filter_by_northwest(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']

    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)

def filter_by_northeast(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']

    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)

def groupby_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'), index=False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smoker'), index=False)

with DAG(
    dag_id= 'branching_with_variables',
    start_date = datetime(2024, 5, 25),
    description='Running branching pipelines with variables',
    tags=['branching', 'variables'],
    schedule='@once',
    catchup=False,
    default_args={
        'owner': 'egi'
    }
):
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    filter_by_southwest = PythonOperator(
        task_id='filter_by_southwest',
        python_callable=filter_by_southwest
    )

    filter_by_southeast = PythonOperator(
        task_id='filter_by_southeast',
        python_callable=filter_by_southeast
    )

    filter_by_northwest = PythonOperator(
        task_id='filter_by_northwest',
        python_callable=filter_by_northwest
    )

    filter_by_northeast = PythonOperator(
        task_id='filter_by_northeast',
        python_callable=filter_by_northeast
    )

    groupby_region_smoker = PythonOperator(
        task_id='groupby_region_smoker',
        python_callable=groupby_region_smoker
    )

    read_csv_file >> remove_null_values >> determine_branch >> [filter_by_southwest, filter_by_southeast, filter_by_northwest, filter_by_northeast, groupby_region_smoker]