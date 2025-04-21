from datetime import datetime, timedelta
from textwrap import dedent

from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

## setting int variable
my_var = Variable.set("param_variable", 10)

with DAG(
    "param_with_get_variable",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={"a": Param(int(Variable.get("param_variable")), type="integer", minimum=0, maximum=20)},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that we are able to use already created variable as a pram with get_variable method.
        Also assert the type of created variable
        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (any float or integer value)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": 22}
                    {"a": 22.1}\n
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than integer)
            - Examples:\n
                    {"a": "other string}
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
        print(f"value of variable is {val} with type as {type(val)}")

        assert type(val) == int

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
