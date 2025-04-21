from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
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

tens_only = [i for i in range(200) if i % 10 == 0]


@task
def fail_if_invalid(val1, val2, val3):
    print(val1, val2, val3)
    assert 20 <= val1 and val2 and val3 <= 200
    assert type(val1 and val2 and val3) == int
    assert val1 % 10 == 0
    assert val2 % 6 == 0
    assert val3 % 15 == 0
    # ... when you forget the modulo character exists
    # assert val1 / 10 == float(str(val1)[0])


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={
        "multiple_of_10": Param(tens_only[2], minimum=20, maximum=200, multipleOf=10),
        "multiple_of_6": Param(tens_only[3], minimum=20, maximum=200, multipleOf=6),
        "multiple_of_15": Param(tens_only[6], minimum=20, maximum=200, multipleOf=15),
    },
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def multiple_of_int_taskflow(of_ten, of_six, of_fifteen):
    fail_if_invalid(of_ten, of_six, of_fifteen)


dag = multiple_of_int_taskflow(50, 72, 75)
