from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
# Purpose

While the datatype used to define the tuple is a list, the prefixItems keyword argument makes it so that the items in the array are only mutable by datatype.
This dag checks that the order of the datatypes in the list stay the same as when they were defined.

## Steps

1. Unpause
2. Wait for dag run to complete
3. Trigger a new dagrun without config
4. Trigger a new dagrun with config, but make no changes
5. Trigger a new dagrun with config, but change it to be some other valid value (a list)
    - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
    - Examples:\n
            {"a": [12, "ted", "22.2]}
            {"a": [22.2, "victor", 33]}\n
6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a list)
    - Examples:\n
            {"a": ["ted", 22, 21]}
            {"a": [23, 34, 56]}

## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API
"""


@task
def fail_if_invalid(val):
    print(val)
    assert type(val[0]) == int or float
    assert type(val[1]) == str
    assert type(val[2]) == int or float
    assert type(val) == list


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={
        "a": Param(
            [1, "ted", 4],
            type="array",
            prefixItems=[
                # you can't directly pass in a tuple but you can make the list immutable in the types
                # here there is some leniency as number types accept both integer and float types
                {"type": "number"},
                {"type": "string"},
                {"type": "number"},
            ],
        )
    },
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def tuple_param_taskflow(json_tuple):
    fail_if_invalid(json_tuple)


dag = tuple_param_taskflow([33.3, "dogs", 22])
