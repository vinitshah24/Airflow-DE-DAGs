from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration


POSTGRES_CONNECTION_ID = "postgres_db_connection"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}


@dag(
    dag_id="sql_operator_with_params_postgres_dag",
    start_date=datetime(2023, 10, 20),
    max_active_runs=3,
    schedule="@daily",
    default_args=default_args,
    template_searchpath="/opt/airflow/include/sql",
    catchup=False,
)
def parameterized_query():

    opr_param_query = SQLExecuteQueryOperator(
        task_id="param_query",
        conn_id=POSTGRES_CONNECTION_ID,
        sql="select_query.sql",
        params={"table_name": "dag_runs"},
        show_return_value_in_logs=True
    )

    opr_param_query


parameterized_query()
