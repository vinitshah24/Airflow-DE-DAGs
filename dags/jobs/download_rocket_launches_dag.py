import json
import pathlib

import requests
import requests.exceptions as requests_exceptions

from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DATA_LOCATION = "/opt/airflow/include"
IMAGE_LOCATION = f"{DATA_LOCATION}/rocket_launch_images"

dag = DAG(
    dag_id="download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval="@daily"
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command=f"curl -o {DATA_LOCATION}/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)


def _get_pictures():
    # Ensure directory exists
    pathlib.Path(IMAGE_LOCATION).mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open(f"{DATA_LOCATION}/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"{IMAGE_LOCATION}/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {IMAGE_LOCATION}/ | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify
