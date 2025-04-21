from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from pendulum import today
from plugins.airflow_dag_introspection import assert_the_task_states

docs = """
####Purpose
This dag tests that the BranchPythonOperator works correctly by testing that xcoms is only returned from the branch that successfully runs it's tasks.\n
It also makes assertions of the tasks states to ensure the tasks that should be skipped are actually skipped.\n
####Expected Behavior
This dag has 7 tasks 5 of which are expected to succeed and 2 of which are expected to be skipped.\n
This dag should pass.

"""


def branch_this_way():
    return "branch1"


def branch1(val):
    return val


def branch2(val):
    return val


def xcoms_check(**context):
    ti = context["ti"]
    val_to_check = ti.xcom_pull(task_ids="branch1", key="return_value")
    should_be_none = ti.xcom_pull(task_ids="branch2", key="return_value")

    assert val_to_check == {"this": "branch", "should": "return"}
    assert should_be_none is None


with DAG(
    dag_id="branch_python_operator", start_date=today("UTC").add(days=-1), schedule=None, doc_md=docs, tags=["core"]
) as dag:
    brancher = BranchPythonOperator(
        task_id="branch_python_operator",
        python_callable=branch_this_way,
    )

    branch1 = PythonOperator(
        task_id="branch1",
        python_callable=branch1,
        op_args=[{"this": "branch", "should": "return"}],
    )

    branch2 = PythonOperator(
        task_id="branch2", python_callable=branch2, op_args=[{"this": "branch", "shouldn't": "return"}]
    )

    d0 = EmptyOperator(task_id="dummy0")

    b0 = BashOperator(task_id="sleep_so_task_skips", bash_command="sleep 25")

    check_xcoms = PythonOperator(
        task_id="check_xcoms",
        python_callable=xcoms_check,
    )

    check_states = PythonOperator(
        task_id="check_task_states",
        python_callable=assert_the_task_states,
        op_kwargs={
            "task_ids_and_assertions": {
                "branch_python_operator": "success",
                "branch1": "success",
                "sleep_so_task_skips": "success",
                "branch2": "skipped",
                "dummy0": "skipped",
            }
        },
    )


brancher >> [branch1, branch2]
branch1 >> b0 >> check_xcoms >> check_states
branch2 >> d0
