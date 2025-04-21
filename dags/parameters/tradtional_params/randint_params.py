from datetime import datetime, timedelta
from random import randint
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

int1 = randint(20, 200)


with DAG(
    "params_randint",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={"a": Param(randint(20, 200), type="integer", minimum=20, maximum=200)},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that the JSON 'integer' or python int datatype interfaces with the dag parameter 'params'.
        It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.
        Additionally it checks that the integers are either equal to the minimum and maximums or they're between the minimums and maximums.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (any integer greater than 20 and less than 200 including 20 and 200)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": 22}
                    {"a": 200}
                    {"a": 20}
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than an integer)
            - Examples:\n
                    {"a": "twentytwo}
                    {"a": [1,3,4]}
                    {"a": {"key": "val"}}

        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API
        """
    ),
    tags=["core", "dagparams"],
) as dag:

    def fail_if_invalid(val):
        print(val)
        assert 20 <= val <= 200
        assert type(val) == int

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
