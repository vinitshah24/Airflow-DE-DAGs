from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime


@dag(
    start_date=datetime(2022, 8, 1),
    schedule=None,
    catchup=False
)
def bash_run_env_dag():

    # tee writes to stdout and to the log file
    # AIRFLOW_HOME = /opt/airflow
    cmd = "echo KEY=${ENV_KEY} && echo NUM=${ENV_NUM} | tee ${AIRFLOW_HOME}/include/data.txt"

    run_task = BashOperator(
        task_id="bash_env_data_task",
        bash_command=cmd,
        env={"ENV_KEY": "Sam", "ENV_NUM": "2024"},
        # append_env=True parameter ensures that the task's environment variables are added
        # to the existing environment variables instead of replacing them.
        append_env=True,
    )

    cat_task = BashOperator(
        task_id="bash_env_cat_task",
        bash_command="cat ${AIRFLOW_HOME}/include/data.txt",
    )

    run_task >> cat_task


bash_run_env_dag()
