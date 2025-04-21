from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

# unpausing causes this dag to run once, because there's only been
# one complete interval of 30 years since the epoch.
thirty_years = 1577862000  # seconds
"""
int1 = randint(20, 200)
def true_or_false(bool):
    if int1 % 2 == 0:
        return True
    else:
        return False
"""
tens_only = [i for i in range(200) if i % 10 == 0]

with DAG(
    "params_int_multiple_of",
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(seconds=thirty_years),
    params={
        "multiple_of_10": Param(tens_only[2], minimum=20, maximum=200, multipleOf=10),
        "multiple_of_6": Param(tens_only[3], minimum=20, maximum=200, multipleOf=6),
        "multiple_of_15": Param(tens_only[6], minimum=20, maximum=200, multipleOf=15),
    },
    render_template_as_native_obj=True,
    doc_md=dedent(
        """
        # Purpose

        Checks that the JSON 'integer' or python int datatype interfaces with the dag parameter 'params'.
        It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.
        Furthermore it asserts that the integers being passed into params are between or equal to the minimum and maximun integers allowed.
        Additionally it tests that the 'multipleOf' keyword argument checks to make sure that the integer being is passed in is a multiple of what was defined in the keyword argument.

        ## Steps

        1. Unpause
        2. Wait for dag run to complete
        3. Trigger a new dagrun without config
        4. Trigger a new dagrun with config, but make no changes
        5. Trigger a new dagrun with config, but change it to be some other valid value\n
            -Three integers where the 1st is a multiple of 10, the 2nd is a multiple of 6 and the the third is a multiple of 15 and they are also all between 20 and 200\n
            - Ensure that the keys remains as "multiple_of_10, multiple_of_6 and multiple_of_15" so the task can make an assertion on its datatype\n
            - Examples:\n
                    {"multiple_of_10": 50, "multiple_of_6": 30, "multiple_of_15": 105}
                    {"multiple_of_10": 110, "multiple_of_6": 72, "multiple_of_15": 135}\n
        6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than an integer or an integer)\n
            - Examples:\n
                    {"multiple_of_10": "string", "multiple_of_6": 201, "multiple_of_15": [1,2,3]}\n
                    {"multiple_of_10": 206, "multiple_of_6": 30, "multiple_of_15": 105}\n

        ## Expectations

        - **2 - 5**: All tasks succeed
        - **6**: The dagrun is never created, user sees validation error in UI or gets error via API
        """
    ),
    tags=["core", "dagparams"],
) as dag:

    def fail_if_invalid(val1, val2, val3):
        print(val1, val2, val3)
        assert 20 <= val1 and val2 and val3 <= 200
        assert type(val1 and val2 and val3) == int

    PythonOperator(
        task_id="ref_param",
        python_callable=fail_if_invalid,
        op_args=[
            "{{ params['multiple_of_10'] }}",
            "{{ params['multiple_of_6'] }}",
            "{{ params['multiple_of_15'] }}",
        ],
    )
