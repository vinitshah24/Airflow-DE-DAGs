import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

BASE_DIR = "/opt/airflow/include/data_pipeline"
RAW_DIR = f"{BASE_DIR}/raw_data"
TRANSFORMED_DIR = f"{BASE_DIR}/transformed_data"
WAREHOUSE_DIR = f"{BASE_DIR}/warehouse"
REPORTS_DIR = f"{BASE_DIR}/reports"

# Create directories if not present
for d in [RAW_DIR, TRANSFORMED_DIR, WAREHOUSE_DIR, REPORTS_DIR]:
    os.makedirs(d, exist_ok=True)


def extract_data(curr_date, **kwargs):
    # Simulate API call by generating dummy data
    data = pd.DataFrame({
        "date": [curr_date] * 5,
        "user_id": range(1, 6),
        "purchase_amount": [100, 150, 200, 50, 300]
    })
    data.to_csv(f"{RAW_DIR}/raw_{curr_date}.csv", index=False)


def transform_data(curr_date, **kwargs):
    df = pd.read_csv(f"{RAW_DIR}/raw_{curr_date}.csv")
    df["purchase_amount"] = df["purchase_amount"] * 1.1  # Apply tax?
    df["is_high_value"] = df["purchase_amount"] > 200
    df.to_csv(f"{TRANSFORMED_DIR}/transformed_{curr_date}.csv", index=False)


def load_data(curr_date, **kwargs):
    df = pd.read_csv(f"{TRANSFORMED_DIR}/transformed_{curr_date}.csv")
    warehouse_file = f"{WAREHOUSE_DIR}/warehouse.csv"
    if os.path.exists(warehouse_file):
        df_existing = pd.read_csv(warehouse_file)
        df = pd.concat([df_existing, df])
    df.to_csv(warehouse_file, index=False)


def send_daily_report(curr_date, **kwargs):
    df = pd.read_csv(f"{TRANSFORMED_DIR}/transformed_{curr_date}.csv")
    report = df.groupby("is_high_value").agg({"purchase_amount": "sum"}).reset_index()
    report.to_csv(f"{REPORTS_DIR}/report_{curr_date}.csv", index=False)


def run_reconciliation(curr_date, prev_date, **kwargs):
    try:
        # Load current and previous day"s data
        today = pd.read_csv(f"{WAREHOUSE_DIR}/warehouse.csv")
        if curr_date not in today["date"].values or prev_date not in today["date"].values:
            raise ValueError(f"Missing data for reconciliation: {curr_date} or {prev_date}")

        today_total = today[today["date"] == curr_date]["purchase_amount"].sum()
        yesterday_total = today[today["date"] == prev_date]["purchase_amount"].sum()

        # Compare totals as a simple reconciliation check
        if abs(today_total - yesterday_total) > 1000:
            raise ValueError(
                f"Reconciliation failed: diff too large ({today_total} vs {yesterday_total})")
    except Exception as e:
        raise RuntimeError(f"Reconciliation failed: {str(e)}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="depend_on_past_data_pipeline_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 10),
    catchup=True,
    max_active_runs=1,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        op_kwargs={"curr_date": "{{ ds }}"},
        # Context is automatically passed in Airflow 2.0+
        # provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={"curr_date": "{{ ds }}"},
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_data,
        op_kwargs={"curr_date": "{{ ds }}"},
    )

    report = PythonOperator(
        task_id="send_report",
        python_callable=send_daily_report,
        op_kwargs={"curr_date": "{{ ds }}"},
    )

    reconcile = PythonOperator(
        task_id="run_reconciliation",
        python_callable=run_reconciliation,
        # This task will depend on the previous run's completion
        # and will use the previous day's data for reconciliation.
        depends_on_past=True, # Ensure this is only for tasks requiring dependency on past runs,
        op_kwargs={"prev_date": "{{ prev_ds }}", "curr_date": "{{ ds }}"}
    )

    extract >> transform >> load >> [report, reconcile]
