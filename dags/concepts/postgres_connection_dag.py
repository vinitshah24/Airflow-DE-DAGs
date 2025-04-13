from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Connection should be already defined in the Airflow UI or environment
# host = postgres
# user = airflow
# password = airflow
# schema = test_db (this is database name)
# port = 5432

POSTGRES_CONNECTION_ID = "postgres_db_connection"

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='postgres_dag',
    default_args=default_args,
    start_date=datetime(2025, 4, 11),
    # This specific cron expression schedules the DAG to run once daily at midnight (00:00 UTC)
    schedule_interval='0 0 * * *'
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id CHARACTER VARYING,
                PRIMARY KEY (dt, dag_id)
            )
        """
    )

    delete_data = PostgresOperator(
        task_id='delete_data',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql="""DELETE FROM dag_runs WHERE dt = '{{ ds }}' AND dag_id = '{{ dag.dag_id }}';"""
    )

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql="""INSERT INTO dag_runs (dt, dag_id) VALUES ('{{ ds }}', '{{ dag.dag_id }}')"""
    )

    # Task dependencies
    create_table >> delete_data >> insert_data