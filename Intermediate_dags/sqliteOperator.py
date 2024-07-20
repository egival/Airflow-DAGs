#we will create table in our Sqlite database
#then we will add values
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import date, datetime, timedelta

with DAG(
    dag_id= 'executing_sql_pipeline',
    start_date = datetime(2024, 3, 31),
    description='Pipeline using SQL operators',
    tags=['pipeline', 'sql'],
    schedule='@once',
    catchup=False,
    default_args={
        'owner': 'egi'
    }
):
    task1_create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER NOT NULL, 
                city VARCHAR(50),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        )

    task2_insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users(name, age, is_active) VALUES
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'my_sqlite_conn'
    )
    
    #is_active will be set to default value of TRUE
    task3_insert_values_2 = SqliteOperator(
        task_id='insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES
                ('Harry', 49), 
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        sqlite_conn_id = 'my_sqlite_conn'
    )
#for all inserted values "created_at" will be set to current timestamp

    task4_delete_values = SqliteOperator(
        task_id = 'delete_values',
        sql = r"""
            DELETE FROM users WHERE is_active = 0;
            """,
        sqlite_conn_id = 'my_sqlite_conn'
    )

    task5_update_values = SqliteOperator(
        task_id = 'update_values',
        sql = r"""
            UPDATE users SET city = 'Seatle';
        """,
        sqlite_conn_id = 'my_sqlite_conn'
    )

    task6_display_result = SqliteOperator(
        task_id = 'display_result',
        sql= r"""SELECT * FROM users""",
        sqlite_conn_id = 'my_sqlite_conn',
        do_xcom_push = True #result of SELECT statement will be available in the XCOM
    )

task1_create_table >> [task2_insert_values_1, task3_insert_values_2] >> task4_delete_values >> task5_update_values >> task6_display_result
