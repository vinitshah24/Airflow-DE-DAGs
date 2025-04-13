from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime


def _training_model():
    accuracy = uniform(0.1, 10.0)
    print(f"model's accuracy: {accuracy}")


# def _training_model(ti):
#     accuracy = uniform(0.1, 10.0)
#     print(f"model's accuracy: {accuracy}")
#     return accuracy  # here


def _choose_best_model():
    print('choose best model')


with DAG('xcom_dag',
         start_date=datetime(2023, 1, 1),
         schedule='@daily',
         catchup=False):

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    training_model_task = [
        PythonOperator(
            task_id=f'training_model_{task}',
            python_callable=_training_model
        ) for task in ['A', 'B', 'C']
    ]

    choose_model = PythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )

downloading_data >> training_model_task >> choose_model
