from datetime import datetime, timedelta
from random import randint
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

int1 = randint(20, 200)
bool1 = []
bool1.append(True) if int1 % 2 == 0 else bool1.append(False)

with DAG(
    "params_bools",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={"a": Param(bool1[0], type="boolean")},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that the python boolean datatype interfaces with the dag parameter params.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value ('true' or 'false') dont forget in JSON booleans aren't capitalized
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": true} {"a": false}\n
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a bool)
            - Examples:\n
                    {"a": "other string}
                    {"a": 23}
                    {"a": 22.1}
                    {"a": ["hello", "world"]}
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
        assert type(val) == bool

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
