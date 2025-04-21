from datetime import datetime, timedelta
from random import random

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
# Purpose

Checks that the JSON 'number' or python int or float datatype interfaces with the dag parameter 'params'.
It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.

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
6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a float or integer)
    - Examples:\n
            {"a": "other string}
            {"a": ["hello", "world"]}
            {"a": {"key1": "val1", "key2": "val2"}}\n

## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API
"""


@task
def fail_if_invalid(val):
    print(val)
    assert type(val) == float or type(val) == int


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={"a": Param(random(), type="number")},
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def float_params_taskflow(flt):
    fail_if_invalid(flt)


dag = float_params_taskflow(1.22)
