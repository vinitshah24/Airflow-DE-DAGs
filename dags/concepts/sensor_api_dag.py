from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow.sensors.python import PythonSensor


@dag(
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False,
    tags=["sensor"],
    dag_id="python_sensor_api_dag"
)
def sensor_dag():

    def check_shibe_availability_func(**context):
        """
        Returns True when the desired condition is met, allowing the task to succeed.
        If False, the sensor will continue to wait and poke again after specified interval.
        """
        r = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
        print(r.status_code)

        # set the condition to True if the API response was 200
        if r.status_code == 200:
            operator_return_value = r.json()
            # pushing the link to the Shibe picture to XCom
            context["ti"].xcom_push(key="return_value", value=operator_return_value)
            return True
        else:
            operator_return_value = None
            print(f"Shibe URL returned the status code {r.status_code}")
            return False

    # poke_interval: This specifies the time (in seconds) between consecutive calls to the
    # python_callable. In this case, the sensor will "poke" the callable every 10 seconds
    # to check if the condition is met.

    # timeout: This defines the maximum time (in seconds) the sensor will wait for the condition
    # to be satisfied. If the callable does not return True within 3600 seconds (1 hour),
    # the task will fail due to a timeout.

    # mode: The mode determines how the sensor behaves while waiting.
    # The "reschedule" mode ensures that the task does not occupy a worker slot while waiting,
    # making it more resource-efficient. Instead of continuously running, the task is rescheduled
    # after each poke_interval.

    check_shibe_availability = PythonSensor(
        task_id="check_shibe_availability",
        poke_interval=10,
        timeout=3600,
        mode="reschedule",
        python_callable=check_shibe_availability_func,
    )

    @task
    def print_shibe_picture_url(url):
        print(url)

    print_shibe_picture_url(check_shibe_availability.output)


sensor_dag()
