from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import DAG
from pendulum import today

with DAG(
    dag_id="skip_mixin_task",
    default_args={"owner": "airflow", "start_date": today("UTC").add(days=-2)},
    schedule=None,
    tags=["core"],
) as dag:

    def needs_some_extra_task(some_bool_field, **kwargs):
        if some_bool_field:
            return "extra_task"
        else:
            return "final_task"

    branch_op = BranchPythonOperator(
        task_id="branch_task",
        python_callable=needs_some_extra_task,
        op_kwargs={"some_bool_field": True},  # For purposes of showing the problem
    )

    # will be always ran in this example
    extra_op = EmptyOperator(
        task_id="extra_task",
    )
    extra_op.set_upstream(branch_op)

    # should not be skipped
    final_op = EmptyOperator(
        task_id="final_task",
        trigger_rule="all_skipped",
    )
    final_op.set_upstream([extra_op, branch_op])
