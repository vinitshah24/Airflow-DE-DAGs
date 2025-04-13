from urllib import request
import time

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


DATA_LOCATION = "/opt/airflow/include/stock_market_data"
POSTGRES_CONNECTION_ID = "postgres_db_connection"


dag = DAG(
    dag_id="stocks_sentiment_analysis",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath=DATA_LOCATION,
    max_active_runs=1,
)


def _get_data(year, month, day, hour, output_path):
    try:
        base_url = "https://dumps.wikimedia.org/other/pageviews"
        query = f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        url = f"{base_url}/{query}"
        request.urlretrieve(url, output_path)
    except Exception as e:
        print(f"Exception occured while fetching data: {e}")


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": f"{DATA_LOCATION}/wikipageviews.gz",
    },
    dag=dag,
)


extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command=f"gunzip --force {DATA_LOCATION}/wikipageviews.gz",
    dag=dag
)


def _fetch_pageviews(pagenames, execution_date):
    result = dict.fromkeys(pagenames, 0)
    with open(f"{DATA_LOCATION}/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open(f"{DATA_LOCATION}/insert_query.sql", "w+") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                f"INSERT INTO pageview_counts VALUES "
                f"('{pagename}', {pageviewcount}, '{execution_date}');\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag,
)

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id=POSTGRES_CONNECTION_ID,
    sql="create_query.sql",
    dag=dag
)

write_to_table = PostgresOperator(
    task_id="write_to_table",
    postgres_conn_id=POSTGRES_CONNECTION_ID,
    sql="insert_query.sql",
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews >> create_table >> write_to_table
