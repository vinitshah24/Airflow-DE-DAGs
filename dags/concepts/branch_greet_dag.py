import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator


with DAG(
    dag_id='python_branch_greet_dag',
    params={
        "name": "hello_name_task"
    },
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0,
    },
    description='A basic DAG branching example',
    schedule_interval=None,  # No Schedule required
    start_date=days_ago(0),  # Don't backdate executions
) as dag:

    def hello_stranger():
        logging.info("Hello Stranger")

    def hello_name(**context):
        logging.info(f"context: {context}")
        logging.info(f"Hello {context['params']['name']}")

    def branching_function(**context):
        logging.info(f"{context['params']}")
        if "name" in context['params']:
            return "hello_name_task"
        else:
            return "hello_stranger_task"

    branching_function_task = BranchPythonOperator(
        task_id=f"branching_function_task",
        python_callable=branching_function,
    )

    hello_name_task = PythonOperator(
        task_id=f'hello_name_task',
        python_callable=hello_name,
    )

    hello_stranger_task = PythonOperator(
        task_id=f'hello_stranger_task',
        python_callable=hello_stranger,
    )

# Set dependency relationship
branching_function_task >> [hello_name_task, hello_stranger_task]
