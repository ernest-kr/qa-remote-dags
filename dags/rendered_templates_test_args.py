from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

twos_day = datetime.now() - timedelta(days=2)


def test_op_args(*args):
    return f"The data interval start date is: {args[-1]} and the data interval end date is {args[-2]}"


default_args = {"owner": "airflow", "depends_on_past": True}

with DAG(
    dag_id="rendered_templates_op_args",
    start_date=twos_day,
    schedule=None,
    tags=["core"],
) as dag:
    temp_op_args = PythonOperator(
        task_id="templated_op_args",
        python_callable=test_op_args,
        op_args=["{{ data_interval_start }}", "{{ data_interval_end }}"],
    )


temp_op_args
