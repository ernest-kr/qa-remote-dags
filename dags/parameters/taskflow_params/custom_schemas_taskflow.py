from datetime import datetime, timedelta
from random import choice

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
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
            {"a": 22.1}
            {"a": ["hello", "world"]}\n
6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than an integer, flaot or list)
    - Examples:\n
            {"a": true}
            {"a": {"key1": 4, "key2": 6}}

## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API

"""

most_types = ["some string", 34, 32.1, ["hello", "QA team"]]


@task
def fail_if_invalid(val):
    print(val)
    assert type(val) in [str, int, list]


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={
        "a": Param(
            # choice picks a random item from a sequence: list, set, dict.values, dict.keys
            choice(most_types),
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
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def custom_schemas_taskflow(custom):
    fail_if_invalid(custom)


dag = custom_schemas_taskflow(["one", "two", "three"])
