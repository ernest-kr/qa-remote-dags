from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

ls1 = [1, 2, "ted", "ned", 4, 5]
with DAG(
    "params_lists",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={"a": Param(ls1, type="array")},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that the JSON 'array' or python list datatype interfaces with the dag parameter 'params'.
        It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (a list)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": [3, "different", ["data", "types"]]}
                    {"a": [1,2,3s]}\n
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a list)
            - Examples"\n
                    {"a": "other string}
                    {"a": 23}
                    {"a": 22.1}
                    {"a": {"key1": "val1", "key2": "val2"}}\n
        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API
        """
    ),
    tags=["core", "dagparams"],
) as dag:

    def fail_if_invalid(val):
        print(val)
        assert type(val) == list

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
