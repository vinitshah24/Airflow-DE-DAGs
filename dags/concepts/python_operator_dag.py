"""
Max XCOM data size = 48MB
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(data_dict, task_instance):
    print("some dict: ", data_dict)
    first_name = task_instance.xcom_pull(task_ids='get_name', key='first_name')
    last_name = task_instance.xcom_pull(task_ids='get_name', key='last_name')
    age = task_instance.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, and I am {age} years old!")


def get_name(task_instance):
    task_instance.xcom_push(key='first_name', value='Sam')
    task_instance.xcom_push(key='last_name', value='Sulek')


def get_age(task_instance):
    task_instance.xcom_push(key='age', value=21)


with DAG(
    default_args=default_args,
    dag_id='python_operator_dag',
    description='Dag using python operator',
    start_date=datetime(2023, 10, 19),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'data_dict': {'country': 'USA', 'year': 2023}}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1
