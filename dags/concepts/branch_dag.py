from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

import random
from pendulum import datetime


@dag(
    dag_id="python_branch_dag",
    start_date=datetime(2025, 4, 1),
    catchup=False,
    schedule="@daily"
)
def branch_python_operator_decorator_example():

    run_this_first = EmptyOperator(task_id="run_this_first")

    task_list = ["task_a", "task_b", "task_c", "task_d"]

    @task.branch(task_id="branching")
    def random_choice(choices):
        # Retuns the random name of the task ID to be executed next
        return random.choice(choices)

    random_choice_instance = random_choice(choices=task_list)

    run_this_first >> random_choice_instance

    cleanup_task = EmptyOperator(
        task_id="cleanup_task",
        trigger_rule="none_failed_min_one_success"
    )

    for curr_task in task_list:
        t = EmptyOperator(task_id=curr_task)
        empty_follow = EmptyOperator(task_id=f"follow_{curr_task}")
        # Label adds name to the arrows (it can help identify more complex branches)
        random_choice_instance >> Label(curr_task) >> t >> empty_follow >> cleanup_task


branch_python_operator_decorator_example()
