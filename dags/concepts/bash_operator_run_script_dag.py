from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime


@dag(
    start_date=datetime(2022, 8, 1),
    schedule=None,
    catchup=False
)
def bash_script_run_dag():
    execute_script_task = BashOperator(
        task_id="bash_execute_script_task",
        # Note the space at the end of the command!
        # AIRFLOW_HOME = /opt/airflow
        bash_command="${AIRFLOW_HOME}/include/shell/list_files.sh "
        # since the env argument is not specified, this instance of the
        # BashOperator has access to the environment variables of the Airflow
        # instance like AIRFLOW_HOME
    )
    execute_script_task


bash_script_run_dag()
