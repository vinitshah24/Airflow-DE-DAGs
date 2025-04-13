from pendulum import datetime
from airflow.decorators import dag, task
from hooks.cat_fact_hook import CatFactHook


@dag(
    dag_id="custom_hook_run_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 1),
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    catchup=False,
)
def custom_hook_run_dag():

    @task
    def get_num_cat_facts_task(number):
        # instatiating a CatFactHook at runtime of this task
        hook = CatFactHook("cat_fact_conn")
        hook.log_cat_facts(round(number))

    get_num_cat_facts_task(2)


custom_hook_run_dag()
