from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

with DAG(
    "params_int_fivetoten",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={"a": Param(7, type="integer", minimum=5, maximum=10)},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that the JSON 'integer' or python int datatype interfaces with the dag parameter 'params'.
        It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (an integer between 5 and 10 including 5 and 10)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": 5}
                    {"a": 6
                    {"a": 7}
                    {"a": 8}
                    {"a": 9}
                    {"a": 10}\n
        6. Trigger a new dagrun with config, but change it to be some invalid value (an integer less than 5 or greater than 10 or any other datatype that's not an integer)
            - Examples:\n
                    {"a": "other string}
                    {"a": 23} {"a": 22.1}
                    {"a": ["hello", "world"]}
                    {"a": {"key1": "val1", "key2": "val2"}}\n

        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API
        """
    ),
    tags=["core", "dagparams"],
) as use_params:

    def fail_if_invalid(x):
        print(x)
        assert 5 <= x <= 10

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
