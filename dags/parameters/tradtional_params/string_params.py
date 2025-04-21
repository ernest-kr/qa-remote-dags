from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds

with DAG(
    "params_strings",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    # pattern is a regular expression to match against the string
    # the regex is matching any character in the string
    params={"a": Param("3tHjh32k9l8q", type="string", minLength=2, maxLength=23, pattern=r"[\s\S]")},
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that the JSON 'string' or python str datatype interfaces with the dag parameter 'params'.
        It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.
        Additionally the function fail_if_invalid makes sure that the character length of the string is between 2 and 23
        The pattern keyword argument is a regex search that matches everything so it should always pass.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value (any string with a character length between 2 and 23)
            - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"a": "coffee"}
                    {"a": "donuts"}
                    {"a": "muffins"}
                    {"a" "9fj0m68k2l71if1sczi5"}
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a string)
            - Examples:\n
                    {"a": 34}
                    {"a": [1, "ted", 2, "tee"]}
                    {"a": {"new_dict": "some_value"}}

        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API
        """
    ),
    tags=["core", "dagparams"],
) as dag:

    def fail_if_invalid(val):
        print(val)
        assert type(val) == str

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=["{{ params['a'] }}"],
    )
