from datetime import datetime, timedelta
from random import randint

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
# Ensure that this commit is merged in the version of airflow you're using before testing otherwise the dag will fail on falsy values:
https://github.com/apache/airflow/pull/22964

# Purpose

Checks that the python boolean datatype interfaces with the dag parameter params.

## Steps

1. Unpause
2. Wait for dag run to complete
3. Trigger a new dagrun without config
4. Trigger a new dagrun with config, but make no changes
5. Trigger a new dagrun with config, but change it to be some other valid value ('true' or 'false') dont forget in JSON booleans aren't capitalized
    - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
    - Examples:\n
            {"a": true} {"a": false}\n
6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a bool)
    - Examples:\n
            {"a": "other string}
            {"a": 23}
            {"a": 22.1}
            {"a": ["hello", "world"]}
            {"a": {"key1": "val1", "key2": "val2"}}\n

## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API
"""

int1 = randint(20, 200)
bool1 = []
bools = bool1.append(True) if int1 % 2 == 0 else bool1.append(False)


@task
def fail_if_invalid(val):
    print(val)
    assert type(val[0]) == bool


@dag(
    start_date=datetime(1970, 1, 1),
    schedule=timedelta(days=365 * 30),
    params={"val": Param(False, type="boolean")},
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def bool_taskflow_test(bool_val):
    # val is a DagParam object
    # print(val)  # <airflow.models.param.DagParam object at 0x103360b50>

    # the task dereferences it
    fail_if_invalid(bool_val)


the_dag = bool_taskflow_test(bool1)
