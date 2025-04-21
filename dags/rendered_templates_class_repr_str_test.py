from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

twos_day = datetime.now() - timedelta(days=2)


class CustomCLass:
    def __init__(self, x, z):
        self.x = x  # "{{ says_hi }}"
        self.z = z  # "{{ the_planets }}"

    def __str__(self):
        return f"The templated jinja variable inside the 'str' method is as follows: {self.x}"

    def __repr__(self):
        return f"The templated jinja variable inside the 'repr' method is as follows: {self.z}"


default_args = {"owner": "airflow", "depends_on_past": True}

with DAG(
    dag_id="template_class_test",
    start_date=twos_day,
    schedule=None,
    render_template_as_native_obj=True,
    user_defined_macros={"my_class": CustomCLass},
    tags=["core"],
) as dag:
    t1 = PythonOperator(
        task_id="test_str_and_repr_method_for_templating",
        python_callable=lambda x: print(x),
        op_args=["""{{ my_class("Hello", ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Neptune"]) }}"""],
    )

    t2 = PythonOperator(
        task_id="test_op_args_templating", python_callable=lambda x: print(x), op_args=["{{ dag_run }}"]
    )

t1 >> t2
