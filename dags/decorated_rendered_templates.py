from datetime import datetime, timedelta

from airflow import settings
from airflow.decorators import task
from airflow.models.renderedtifields import RenderedTaskInstanceFields as rtif
from airflow.sdk import DAG

docs = """
####Purpose
The purpose of this dag is to check that rendered templates, which are found in the task details in the UI, are rendering the correct datatypes with python decorated tasks.\n
It achieves this test by using the RenderedTaskInstanceFields from airflow.models.renderedtifields to assert the xcom arg datatypes.
####Expected Behavior
This dag has 3 tasks that are all expected to succeed.
"""


@task
def pusher1(dict1):
    return dict1


@task
def pusher2(template2):
    return template2


@task
def templated1(**context):
    sesh = settings.Session()
    dagrun = context["dag_run"]
    get_ti1 = dagrun.get_task_instance("pusher1")
    get_ti2 = dagrun.get_task_instance("pusher2")
    temp_fields_task1 = rtif.get_templated_fields(get_ti1, sesh)
    temp_fields_task2 = rtif.get_templated_fields(get_ti2, sesh)
    print(f"This is the 1st tasks rendered_template_fields {temp_fields_task1}")
    print(f"This is the 2nd tasks rendered_template_fields {temp_fields_task2}")
    print(
        f"""
    The 1st tasks xcom arg datatype is {type(temp_fields_task1['op_args'][0])}
    And the 2nd tasks xcom arg datatype is {type(temp_fields_task2['op_args'][0])}
    """
    )

    assert isinstance(temp_fields_task1["op_args"][0], list) is True
    assert isinstance(temp_fields_task2["op_args"][0], dict) is True
    assert temp_fields_task1["op_args"][0] == ["hello_world", "01234567-8910-1112-1314-151617181920"]
    assert float(temp_fields_task2["op_args"][0]["key1"]) > 0 and float(temp_fields_task2["op_args"][0]["key1"]) < 1


with DAG(
    dag_id="check_decorated_rendered_templates",
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=61),
    doc_md=docs,
    max_active_runs=3,
    tags=["core"],
) as dag:
    t1 = pusher1(["hello_world", '{{ macros.uuid.UUID("01234567891011121314151617181920") }}'])
    t2 = pusher2({"key1": "{{ macros.random() }}"})
    t3 = templated1()

t1 >> t2 >> t3
