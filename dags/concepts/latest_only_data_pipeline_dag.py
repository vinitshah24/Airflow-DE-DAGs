from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


def extract_data():
    print("🔄 Extracting data from source systems...")


def transform_data():
    print("🛠️ Transforming and aggregating data...")


def load_data():
    print("📥 Loading data into data warehouse...")


def refresh_dashboard():
    print("📊 Hitting dashboard API to refresh visuals...")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "latest_only_data_pipeline_dag",
    default_args=default_args,
    description="ETL + dashboard refresh, but only latest run triggers dashboard",
    schedule_interval="@daily",
    catchup=True,
    tags=["etl", "dashboard"],
) as dag:

    start = DummyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    # This ensures the dashboard is only refreshed on the latest DAG run
    latest_only = LatestOnlyOperator(task_id="latest_only")

    dashboard_refresh = PythonOperator(
        task_id="refresh_dashboard",
        python_callable=refresh_dashboard,
    )

    start >> extract >> transform >> load >> latest_only >> dashboard_refresh
