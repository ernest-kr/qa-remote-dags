import random
from datetime import datetime, timedelta

from airflow.macros import ds_add
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

two_days_ago = datetime.now() - timedelta(days=2)


def py_template1(**kwargs):
    add_to_ds = kwargs["add2start_date"]
    the_ds = kwargs["logical_date"]
    return f"the start date is: {the_ds} and date subtracted is {add_to_ds}"


def py_template2(**kwargs):
    return f"My birthday is: {kwargs['my_bday']}"


def platform_chooser(tup1_executor_dict_item):
    return random.sample(tup1_executor_dict_item, 1)


default_args = {"owner": "airflow", "depends_on_past": True}

with DAG(
    dag_id="rendered_templates_test_16876",
    start_date=two_days_ago,
    schedule=None,
    default_args=default_args,
    # allows you to pipe python functions in a bash_command task parameter, value is the function
    user_defined_filters={"platform_chooser": platform_chooser},
    # allows you to define custom macros to use in jinja templating
    user_defined_macros={
        "trevors_bday": datetime(2021, 3, 26),
        "QA_team_members": ["Matt", "Ernest", "Himabindu", "Jyotsana", "Priyanka", "Isaul"],
        "executor": ("Kubernetes", "Celery", "Sequential"),
        ("tuple", "parenthesis"): ["one", "two"],
    },
    tags=["core"],
) as dag:
    py_template1 = PythonOperator(
        task_id="py_temp1",
        python_callable=py_template1,
        op_kwargs={
            "logical_date": "{{ ds }}",
            # write github issue on apache airflow
            # is only returning the ds_add with 5 days subtracted
            "add2start_date": ds_add("2021-11-10", -36),
        },
    )

    bash_template1 = BashOperator(
        task_id="bash_temp1",
        bash_command="echo the run id is: '{{ run_id }}'",
    )

    py_template2_user_filters = PythonOperator(
        task_id="py_temp2",
        python_callable=py_template2,
        op_kwargs={
            # make qa scenario dags in future
            # "time_diff": datetime_diff_for_humans("{{ ds }}", "{{ macros.datetime.now() }}"),
            "my_bday": "{{ trevors_bday }}",
        },
    )

    bash_template2 = BashOperator(
        task_id="bash_temp2_filters_aka_functions",
        bash_command="echo the executors on which airflow can run on is {{ executor | platform_chooser }}",
    )

py_template1 >> bash_template1 >> py_template2_user_filters >> bash_template2
