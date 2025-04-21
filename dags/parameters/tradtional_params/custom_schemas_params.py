from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

with DAG(
    "params_custom_schemas",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={
        "a": Param(
            "default_param_value",
            # if an incorrect type is hardcoded then an import error occurs (neg test)
            # using the kwarg allOf also gives an import error if two different types are defined (neg test)
            anyOf=[
                {"type": "string", "minLength": 3, "maxLength": 22},
                {"type": "number", "minimum": 0},
                {"type": "array"},
            ],
            type=["string", "number", "array"],
        )
    },
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose
        Checks that, anyOf, a JSON schema composition helper works correctly when using the dag parameter params.
        anyOf is similar to 'or' used in python statements where only one match has to occur for one of the datatypes defined in anyOf


        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (any string, integer, float or list)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": "other string}
                    {"a": 23}
                    {"a": ["hello", "world"]}\n
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than an integer, flaot or list)
            - Examples:\n
                    {"a": true}
                    {"a": 22.1}
                    {"a": {"key1": 4, "key2": 6}}

        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API

        """
    ),
    tags=["core", "dagparams"],
) as dag:

    def fail_if_invalid(val):
        print(f"got '{val}' of type: {type(val)}")
        assert type(val) in [str, int, list]

    py0 = PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
