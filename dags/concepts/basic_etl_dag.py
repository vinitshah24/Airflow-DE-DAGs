import json
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    "basic_etl_dag",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["basic_etl"],
)
def example_dag_basic():

    @task()
    def extract():
        """
        #### Extract task
        A simple "extract" task to get data ready for the rest of the
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(
        multiple_outputs=True
        # multiple_outputs=True unrolls dictionaries into separate XCom values
        # without above, it send xcom as {'total_order_value': 1236.7} so we have parse it
    )
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple "transform" task which takes in the collection of order data
        and computes the total order value.
        """
        total_order_value = 0
        for value in order_data_dict.values():
            total_order_value += value
        # float is not subscriptable
        return {"total_order_value": total_order_value}

    # @task()
    # def load(total_order_value: float):
    #     """
    #     #### Load task
    #     A simple "load" task that takes in the result of the "transform" task
    #     and prints it out, instead of saving it to end user review.
    #     """
    #     print(f"Total order value is: {total_order_value:.2f}")

    @task()
    def load(total_order_value):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task
        and prints it out, instead of saving it to end user review.
        """
        print(f"Total order value is: {total_order_value:.2f}")

    # Call the task functions to instantiate them and infer dependencies
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


# Call the dag function to register the DAG
example_dag_basic()
