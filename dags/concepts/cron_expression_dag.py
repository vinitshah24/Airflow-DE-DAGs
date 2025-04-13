from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="cron_expression_dag",
    start_date=datetime(2025, 4, 1),
    # Run at 09:00 AM on Saturday and Sunday
    schedule_interval='0 9 * * SAT,SUN'
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command="echo 'Running dag with cron expression!'"
    )
