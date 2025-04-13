import datetime as dt
from pathlib import Path

import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


EVENTS_API_HOST = "192.168.4.108"
EVENTS_API_PORT = "5000"
DATA_LOCATION = "/opt/airflow/include/event_data"


dag = DAG(
    dag_id="process_user_event",
    schedule_interval="@daily",
    start_date=dt.datetime(year=2023, month=10, day=21),
    end_date=dt.datetime(year=2023, month=10, day=24),
    catchup=True,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /opt/airflow/include/event_data/events && "
        "curl -o /opt/airflow/include/event_data/events/{{ds}}.json "
        "http://192.168.4.108:5000/events?start_date={{ds}}&end_date={{next_ds}}"
    ),
    dag=dag,
)


def _calculate_stats(**context):
    """Calculates event statistics."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/opt/airflow/include/event_data/events/{{ds}}.json",
        "output_path": "/opt/airflow/include/event_data/stats/{{ds}}.csv",
    },
    dag=dag,
)


def email_stats(stats, email):
    """Send an email..."""
    print(f"Sending below stats: \n{stats}")
    print(f"Sending stats to {email}...")


def _send_stats(email, **context):
    stats = pd.read_csv(context["templates_dict"]["stats_path"])
    email_stats(stats, email=email)


send_stats = PythonOperator(
    task_id="send_stats",
    python_callable=_send_stats,
    op_kwargs={"email": "user@example.com"},
    templates_dict={"stats_path": "/opt/airflow/include/event_data/stats/{{ds}}.csv"},
    dag=dag,
)

fetch_events >> calculate_stats >> send_stats
