"""Example DAG demonstrating the usage of the @task.short_circuit decorator."""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

from pendulum import datetime


@dag(
    dag_id="python_short_circuit_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
)
def short_circuit_operator_decorator_example():

    @task.short_circuit
    def condition_is_true():
        return True

    @task.short_circuit
    def condition_is_false():
        return False

    ds_true = [EmptyOperator(task_id='true_' + str(i)) for i in [1, 2]]
    ds_false = [EmptyOperator(task_id='false_' + str(i)) for i in [1, 2]]

    chain(condition_is_true(), *ds_true)
    chain(condition_is_false(), *ds_false)


short_circuit_operator_decorator_example()
