from airflow.decorators import dag, task, setup, teardown
from pendulum import datetime


@dag(
    dag_id="setup_teardown_dag",
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False
)
def setup_teardown_run():

    @setup
    def my_cluster_setup_task():
        print("Setting up resources!")
        my_cluster_id = "cluster-2319"
        return my_cluster_id

    @task
    def my_cluster_worker_task():
        return "Doing some work!"

    @teardown
    def my_cluster_teardown_task(my_cluster_id):
        return f"Tearing down {my_cluster_id}!"

    @setup
    def my_database_setup_task():
        print("Setting up my database!")
        my_database_name = "DWH"
        return my_database_name

    @task
    def my_database_worker_task():
        return "Doing some work!"

    @teardown
    def my_database_teardown_task(my_database_name):
        return f"Tearing down {my_database_name}!"

    my_setup_task_obj = my_cluster_setup_task()
    my_setup_task_obj >> my_cluster_worker_task() >> my_cluster_teardown_task(my_setup_task_obj)

    my_database_setup_obj = my_database_setup_task()
    my_database_setup_obj >> my_database_worker_task() >> my_database_teardown_task(my_database_setup_obj)


setup_teardown_run()
