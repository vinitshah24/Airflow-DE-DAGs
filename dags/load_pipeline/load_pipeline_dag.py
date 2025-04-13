from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.postgresql_row_count_operator import PostgreSQLRowCountOperator

import uuid
from datetime import datetime, timedelta
import uuid


QUERIES_DIR = "queries"
POSTGRES_CONN_ID = "postgres_test_db_connection"
SCHEMA_NAME = "test_db"
# Connection should be already defined in the Airflow UI or environment
# host = postgres
# user = airflow
# password = airflow
# schema = test_db (this is database name)
# port = 5432

dags_config = {
    "load_sales_us": {
        "schedule_interval": "@daily",
        "start_date": datetime(2024, 4, 10, 0, 0, 0),
        "table_name": "us_sales"
    },
    "load_sales_uk": {
        "schedule_interval": "@daily",
        "start_date": datetime(2024, 4, 10, 0, 0, 0),
        "table_name": "uk_sales"
    },
    "load_sales_india": {
        "schedule_interval": "@daily",
        "start_date": datetime(2024, 4, 10, 0, 0, 0),
        "table_name": "india_sales"
    }
}


def check_table_exists(table_name):
    schema_check_query = "SELECT * FROM pg_tables;"
    table_check_query = """
    SELECT *
    FROM information_schema.tables
    WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{table_name}';
    """.strip()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # # Retrieves schema name
    # query = hook.get_records(sql=schema_check_query)
    # result_schema = None
    # for result in query:
    #     if "test_db" in result:
    #         result_schema = result[0]
    #         break
    # if not result_schema:
    #     raise ValueError("Schema 'test_db' not found in the database.")
    # Checks if the table exists
    query = hook.get_first(sql=table_check_query)
    print(query)
    if query:
        return "table_exists_task"
    else:
        print(f"Table {table_name} does not exists!")
        return "create_table_task"


def create_dag(dag_id, dag_config):
    table_name = dag_config.get("table_name")
    default_args = {
        "owner": "airflow",
        "start_date": datetime(2025, 3, 1),
        "retries": 0,
        # "retry_delay": timedelta(minutes=1),
    }
    dag = DAG(dag_id=dag_id,
              schedule_interval=dag_config.get("schedule_interval"),
              start_date=dag_config.get("start_date"),
              default_args=default_args,
              catchup=False,
              render_template_as_native_obj=True)
    with dag:
        @task
        def log_process_start(unique_id, table_name):
            print(f"DAG ID {unique_id} processing table {table_name}")

        get_current_user_task = BashOperator(
            task_id="get_current_user_task",
            bash_command="echo 'user'",
            do_xcom_push=True,
            execution_timeout=timedelta(seconds=30),
        )

        table_check_task = BranchPythonOperator(
            task_id="table_check_task",
            python_callable=check_table_exists,
            op_args=[table_name],
        )

        create_table_task = PostgresOperator(
            task_id="create_table_task",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f"{QUERIES_DIR}/create.sql",
            params={"table_name": table_name}
        )

        table_exists_task = EmptyOperator(
            task_id="table_exists_task",
        )

        load_table_task = PostgresOperator(
            task_id="load_table_task",
            postgres_conn_id=POSTGRES_CONN_ID,
            trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
            sql=f"{QUERIES_DIR}/load.sql",
            params={
            "table_name": table_name,
            "id": uuid.uuid4().int % 123456789,
            "load_timestamp": "{{ ts }}"  # Airflow macro for execution timestamp
            }
        )

        row_count_task = PostgreSQLRowCountOperator(
            task_id="row_count_task",
            postgres_conn_id=POSTGRES_CONN_ID,
            table_name=table_name
        )

    log_process_start(unique_id=dag_id, table_name=table_name) >> get_current_user_task >> \
        table_check_task >> [create_table_task, table_exists_task] >> \
        load_table_task >> row_count_task

    return dag


# Creates and registers DAGs
for dag_id in dags_config:
    # globals()[dag_id] =
    create_dag(dag_id=dag_id, dag_config=dags_config[dag_id])
