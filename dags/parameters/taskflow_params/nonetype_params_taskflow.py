from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
# Purpose

Checks that the JSON 'null' or python NoneType datatype interfaces with the dag parameter 'params'.
It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.


## Steps

1. Unpause
2. Wait for dag run to complete
3. Trigger a new dagrun without config
4. Trigger a new dagrun with config, but make no changes
5. Trigger a new dagrun with config, but change it to be some invalid value (any value other than a NoneType value)
    - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
    - Example:\n
            {"a": [1,2,3]}
            {"a": "ted"}
            {"a" {"key": "val"}}

## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API
"""


@task
def fail_if_invalid(val):
    print(val)
    assert val is None


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={"a": Param(None, type="null")},
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def nonetype_params_taskflow(a):
    fail_if_invalid(a)


dag = nonetype_params_taskflow(None)
