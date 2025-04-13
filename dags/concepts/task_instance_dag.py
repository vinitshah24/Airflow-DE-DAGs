from pendulum import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="task_instance_dag",
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
)
def context_and_xcom():
    @task
    def upstream_task(**context):

        print("ts", context["ts"])
        print("execution_date", context["execution_date"])
        print("prev_ds", context["prev_ds"])
        print("next_ds", context["next_ds"])
        print("yesterday_ds", context["yesterday_ds"])

        print("================")
        print("dag_run conf", context["dag_run"].conf)

        print("================")
        print("dag_id", context["dag"].dag_id)
        print("default_args", context["dag"].default_args)
        print("tags", context["dag"].tags)
        print("is_paused", context["dag"].is_paused)
        print("is_active", context["dag"].get_is_active())
        print("is_subdag", context["dag"].is_subdag)

        print("================")
        print("task_id", context["task"].task_id)
        print("task_type", context["task"].task_type)

        print("================")
        print(context["ti"].task_id)
        print(context["ti"].execution_date)
        print(context["ti"].try_number)
        print(context["ti"].max_tries)
        print(context["ti"].start_date)
        print(context["ti"].end_date)
        print(context["ti"].state)
        print(context["ti"].hostname)
        print(context["ti"].log_url)

        context["ti"].xcom_push(key="my_explicitly_pushed_xcom", value=23)
        return 19

    @task
    def downstream_task(passed_num, **context):
        output = context["ti"].xcom_pull(task_ids="upstream_task", key=None)
        print("Output: ", output)
        returned_num = context["ti"].xcom_pull(task_ids="upstream_task",
                                               key="return_value")
        explicit_num = context["ti"].xcom_pull(task_ids="upstream_task",
                                               key="my_explicitly_pushed_xcom")
        print("Returned Num: ", returned_num)
        print("Passed Num: ", passed_num)
        print("Explicit Num: ", explicit_num)

    downstream_task(upstream_task())


context_and_xcom()
