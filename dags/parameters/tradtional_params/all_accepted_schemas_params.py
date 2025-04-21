from datetime import datetime, timedelta
from random import choice
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

all_types = [
    "some string",
    34,
    32.1,
    ["hello", "QA team"],
    {"xz": "xy", "ac": "dc"},
    True,
    False,
    None,
]

with DAG(
    "params_all__default_schemas",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={
        "a": Param(
            choice(all_types),
            type=["string", "number", "array", "object", "boolean", "null"],
        )
    },
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that you can use all the accepted schemas for JSON in the dag parameter Param

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (any string, integer, float, list or dictionary type)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": "other string}
                    {"a": 23} {"a": 22.1}
                    {"a": ["hello", "world"]}
                    {"a": {"key1": "val1", "key2": "val2"}}\n
        ## Expectations

        - **2 - 5**: All tasks succeed
        """
    ),
    tags=["core", "params"],
) as dag:

    def fail_if_invalid(val):
        print(val, "\n", type(val))
        types = [str, int, float, float, list, dict, bool, None]
        if val is None:
            pass
        else:
            assert type(val) in types

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
