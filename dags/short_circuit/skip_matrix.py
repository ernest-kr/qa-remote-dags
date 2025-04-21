from datetime import datetime
from textwrap import dedent

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule
from plugins.airflow_dag_introspection import assert_the_task_states

doc_template = dedent(
    """
    Behavior Under Test
    ===================

    If ShortCircuitOperator's callable returns False, any tasks
    that are downstream of it (but that are not directly
    downstream of another task) should be skipped.  Otherwise
    they should run.

    Expectations
    ------------
    {}
    """
)

ignore_downstream_true = dedent(
    """
    success tasks:
     - start
     - decide

     skipped tasks:
     - join
     - end
     - assert_states
     - skip_me
     - and_me
    """
)

assertion1 = {
    "start": "success",
    "decide": "success",
    "join": "skipped",
    "end": "skipped",
    "skip_me": "skipped",
    "and_me": "skipped",
}

ignore_downstream_false = dedent(
    """
    success tasks:
     - start
     - join
     - end
     - decide
     - assert_states

     skipped tasks:
     - skip_me
     - and_me
    """
)

assertion2 = {
    "start": "success",
    "decide": "success",
    "join": "success",
    "end": "success",
    "skip_me": "skipped",
    "and_me": "skipped",
}


def decide(val):
    return int(val) == 1


# This is the dag factory that makes dags
def get_dag(dag_id, use_value, expectation, boolean, assertion):
    with DAG(
        dag_id=dag_id,
        start_date=datetime(year=1970, month=1, day=1),
        schedule=None,
        tags=["short_circuit", "core"],
        doc_md=doc_template.format(expectation),
    ) as dag:
        start = EmptyOperator(task_id="start")
        # if 'ignore_downstreamm_trigger_rules=False' then downstream trigger rules are respected instead of skipping everything downstream
        join = EmptyOperator(task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
        # this only works on ignore_downstream_trigger_rule=False as when it's set to True downstream trigger rules are ignored
        assert_states = PythonOperator(
            task_id="assert_states",
            python_callable=assert_the_task_states,
            op_args=[assertion],
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # determine the argument to ShortCircuitOperator's callable

        decision = ShortCircuitOperator(
            task_id="decide",
            python_callable=decide,
            op_args=[use_value],
            ignore_downstream_trigger_rules=boolean,
        )

        # split, maybe short circuit one side, then join
        start >> [decision, join]
        (decision >> EmptyOperator(task_id="skip_me") >> EmptyOperator(task_id="and_me") >> join)
        join >> end >> assert_states

        return dag


# two test dags with xcom args
# xcom_skip = get_dag("short_circuit_always_skip_xcom", zero, short_circuted)
# xcom_run = get_dag("short_circuit_always_run_xcom", one, not_short_circuted)

# two test dags with constants
# created with the tasks in the dag factory above ^^
const_skip = get_dag("downstream_trigger_rule_false", 0, ignore_downstream_false, False, assertion2)
const_run = get_dag("downstream_trigger_rule_true", 0, ignore_downstream_true, True, assertion1)
