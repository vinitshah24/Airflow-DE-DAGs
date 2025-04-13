from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


class PostgreSQLRowCountOperator(BaseOperator):
    def __init__(self, table_name, postgres_conn_id, *args, **kwargs):
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        super().__init__(*args, **kwargs)

    def execute(self, context):
        sql_query = "SELECT COUNT(*) FROM {}".format(self.table_name)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        query_result = hook.get_records(sql=sql_query)
        logger.info(f"Query result: {query_result}")
        logger.info(f"Row count for table {self.table_name}: {query_result[0][0]}")
        message = f"{context['run_id']} completed successfully!"
        logger.info(message)
        context['task_instance'].xcom_push(key='result_value', value=query_result)
        context['task_instance'].xcom_push(key='status', value=message)
