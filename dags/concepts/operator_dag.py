from datetime import datetime, timedelta

from airflow.decorators import dag, task


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='taskflow_api_dag',
    default_args=default_args,
    start_date=datetime(2023, 10, 19),
    schedule_interval='@daily'
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Sam',
            'last_name': 'Sulek'
        }

    @task()
    def get_age():
        return 21

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} and I am {age} years old!")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)


hello_world_etl()
