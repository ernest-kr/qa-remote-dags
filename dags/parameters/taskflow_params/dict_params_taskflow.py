from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

docs = """
# Purpose

Checks that the JSON 'object' or python dict datatype interfaces with the dag parameter 'params'.
It does this by asserting that the type of the data passed through is the type that was passed in with 'params'.
## Steps

1. Unpause
2. Wait for dag run to complete
3. Trigger a new dagrun without config
4. Trigger a new dagrun with config, but make no changes
5. Trigger a new dagrun with config, but change it to be some other valid value (any dictionary)
    - Ensure that the key remains as "a" so the task can make an assertion on its datatype\n
    - Since the default config isn't showing up use this data:\n
            {"a": {"hello": "world", "goodbye": "universe"}}\n
    6. Trigger a new dagrun with config, but change it to be some invalid value (any datatype other than a dictionary)\n
    - Examples:\n
            {"a": "some string"}
            {"a": 34}
            {"a": [1, "ted", 2, "tee"]}

## Expectations

- **2 - 5**: All tasks succeed
- **6**: The dagrun is never created, user sees validation error in UI or gets error via API
"""

# doesn't show in config when clicking 'trigger_dag_with config'


@task
def fail_if_invalid(val):
    print(val)
    assert type(val) == dict


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=365, hours=6),
    params={"a": Param({"b": [4, 5, 6], "c": [1, 2, 3]}, type="object")},
    doc_md=docs,
    tags=["core", "taskflow-api", "dagparams"],
)
def dict_params_taskflow(dictionary):
    fail_if_invalid(dictionary)


dag = dict_params_taskflow({"b": 1, "d": 2})
