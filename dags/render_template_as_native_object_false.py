from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

twos_day = datetime.now() - timedelta(days=2)


def native_obj_false(x, y):
    return x, y


def check_type(**context):
    ti = context["ti"]
    pulled_value = ti.xcom_pull(task_ids="py_native_obj_false", key="return_value")
    print(f"This is the datatype for the pulled xcom value: {type(pulled_value)}")
    print(f"This is datatype for the 0th index of the xcom value: {type(pulled_value[0])}")
    print(f"This is datatype for the 1st index of the xcom value: {type(pulled_value[0])}")
    assert type(pulled_value[0]) == str and type(pulled_value[1]) == str


default_args = {"owner": "airflow", "depends_on_past": True}

with DAG(
    dag_id="native_obj_false",
    start_date=twos_day,
    schedule=None,
    # when set to false only the __str__() method is returned
    render_template_as_native_obj=False,
    tags=["core", "negative"],
    user_defined_macros={
        "says_hi": "Hello World!",
        "the_planets": [
            "Mercury",
            "Venus",
            "Earth",
            "Mars",
            "Jupiter",
            "Saturn",
            "Neptune",
        ],
        "ints": [1, 2, 3, 4, 5],
    },
) as dag:
    t1 = PythonOperator(
        task_id="py_native_obj_false",
        python_callable=native_obj_false,
        op_args=["{{ says_hi }}", "{{ ints }}"],
    )

    t2 = PythonOperator(
        task_id="check_xcoms",
        python_callable=check_type,
    )

t1 >> t2
