import calendar
from datetime import date

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.weekday import BranchDayOfWeekOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.airflow_dag_introspection import assert_the_task_states

docs = """
####Purpose
The purpose of this dag is to test that the BranchDayOfWeekOperator works correctly.\n
It achieves this test by dynamically generating the weekday so that it always runs on the current day.\n
Once the BranchDayOfWeekOperator task runs assertions are made that tasks that should be skipped are skipped by checking the task's state.\n
Additionally, the dag makes assertions that xcoms is returned from the branch that isn't skipped and that xcoms isn't returned from the branch that is skipped.
####Expected Behavior
This dag has 7 tasks 5 of which are expected to succeed and 2 of which are expected to be skipped.\n
This dag should pass.
"""


def branch1(val):
    day2 = calendar.day_name[date.today().weekday()]
    print(f"The pendulum day is: {day2}")
    print(type(day2))  # str

    return val


def branch2(val):
    return val


def check_branch1(**context):
    ti = context["ti"]
    val_to_check = ti.xcom_pull(task_ids="branch1", key="return_value")
    should_be_none = ti.xcom_pull(task_ids="branch2", key="return_value")

    assert val_to_check == {"this": "branch", "should": "return"}
    assert should_be_none is None


with DAG(
    dag_id="branch_day_of_week_operator",
    start_date=today("UTC").add(days=-1),
    schedule=None,
    doc_md=docs,
    tags=["core"],
) as dag:
    py0 = PythonOperator(
        task_id="branch1",
        python_callable=branch1,
        op_args=[{"this": "branch", "should": "return"}],
    )

    py1 = PythonOperator(
        task_id="branch2", python_callable=branch2, op_args=[{"this": "branch", "shouldn't": "return"}]
    )

    brancher = BranchDayOfWeekOperator(
        task_id="branch_day_of_week",
        follow_task_ids_if_true="branch1",
        follow_task_ids_if_false="branch2",
        # This ensures it's always ran on the day of week it is.
        week_day=f"{calendar.day_name[date.today().weekday()]}",
    )

    b0 = BashOperator(
        task_id="sleep_so_task_is_skipped",
        bash_command="sleep 25",
    )

    py2 = PythonOperator(
        task_id="check_branch1_xcoms",
        python_callable=check_branch1,
    )

    py3 = PythonOperator(
        task_id="assert_task_states",
        python_callable=assert_the_task_states,
        op_kwargs={
            "task_ids_and_assertions": {
                "branch_day_of_week": "success",
                "branch1": "success",
                "check_branch1_xcoms": "success",
                "branch2": "skipped",
                "dummy0": "skipped",
            }
        },
    )

    d0 = EmptyOperator(task_id="dummy0")


brancher >> [py0, py1]
py1 >> d0
py0 >> b0 >> py2 >> py3
