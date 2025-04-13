INSERT INTO {{params.table_name}} VALUES (
    {{ params.id }},
    ' {{ ti.xcom_pull(key="return_value", task_ids="get_current_user_task") }}' ,
    '{{ params.load_timestamp }}'::timestamp
);
