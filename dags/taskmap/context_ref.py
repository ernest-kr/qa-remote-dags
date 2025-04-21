from datetime import datetime

from airflow.decorators import task
from airflow.sdk import DAG


@task
def a_list():
    return [3, 6, 9]


@task
def ref_context(num, **context):
    return num + context["ti"].map_index * 5
    #  3,  6,    9      # input data
    # +0, +5,  +10      # plus 5 * map_index
    # =3, =11, =19
    # 3 +  11 + 19 = 33  # assert sum(outputs) == 33


@task
def assert_sum(nums, expect):
    print(nums)
    print(f"expecting sum: {' + '.join(map(str,nums))} == {expect}")
    print(sum(nums))
    assert sum(nums) == expect


with DAG(dag_id="context_ref", start_date=datetime(1970, 1, 1), schedule=None, tags=["taskmap"]) as dag:
    assert_sum(ref_context.expand(num=a_list()), 33)
