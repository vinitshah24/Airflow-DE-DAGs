from ast import In
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# catchup => if false, it sequentially runs for each interval between start_date and curr_date
# In below, if the DAG is picked up by the scheduler daemon on 2023-10-19 at 6 AM,
# (or from the command line), a single DAG Run will be created with a data
# between 2023-10-18 and 2023-10-19, and the next one will be created just after midnight on
# the morning of 2023-10-20 with a data interval between 2023-10-19 and 2023-10-20.

with DAG(
    dag_id='catchup_backfill_dag',
    default_args=default_args,
    start_date=datetime(year=2022, month=10, day=1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command="echo 'Hello World!'"
    )
