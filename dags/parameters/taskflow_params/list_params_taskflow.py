from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
# Purpose

Checks that the JSON 'array' or python list datatype interfaces with the dag parameter 'params'.
It does this by asserting that the type of the data passed through to the function is the type that was passed in with 'params'.

## Steps

1. Unpause
2. Wait for dag run to complete
3. Trigger a new dagrun without config
4. Trigger a new dagrun with config, but make no changes
5. Trigger a new dagrun with config, but change it to be some other valid value (a list)
    - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
    - Examples:\n
            {"a": [3, "different", ["data", "types"]]}
            {"a": [1,2,3s]}\n
6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a list)
    - Examples"\n
            {"a": "other string}
            {"a": 23}
            {"a": 22.1}
            {"a": {"key1": "val1", "key2": "val2"}}\n
## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API
"""


@task
def fail_if_invalid(val):
    print(val)
    assert type(val) == list


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={"a": Param([1, 2, "ted", "ned", 4, 5], type="array")},
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def list_params_taskflow(ls):
    fail_if_invalid(ls)


dag = list_params_taskflow(["one", "two", 3, "ned", 4, "ted"])
