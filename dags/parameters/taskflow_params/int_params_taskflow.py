from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
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


@task
def fail_if_invalid(x):
    print(x)
    assert 5 <= x <= 10


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={"a": Param(7, type="integer", minimum=5, maximum=10)},
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def int_params_taskflow(integer):
    fail_if_invalid(integer)


dag = int_params_taskflow(8 or 9)
