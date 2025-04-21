from datetime import datetime, time, timedelta
from uuid import uuid4

from airflow.macros import datetime_diff_for_humans, ds_add, ds_format, random
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dateutil.parser import *
from pendulum import today

docs = """
####Purpose
This dag tests that airflow's builtin macros work correctly. Currently 'macros.time' and 'macros.random' are having issues
####Expected Behavior
This dag has 7 tasks 2 of which are expected to fail until a bug is fixed. The tasks that are expected to fail are 'test_time_macros' and 'test_random_macros'.\n
If the tasks 'test_time_macros' or 'test_random_macros' succeeds then a bug has been fixed.\n
All other tasks should succed if any other tasks fails there is a problem with that macro the task is associated with.
"""

"""
List of builtin macros
macros.datetime - datetime.datetime
macros.timedelta - datetime.timedelta
macros.datetutil - dateutil package
macros.time - datetime.time
macros.uuid - python standard lib uuid
macros.random - python standard lib random
"""


def date_time(datetime_obj):
    compare_obj = datetime(2021, 12, 12, 8, 32, 23)
    assert datetime_obj == compare_obj


def time_delta(timedelta_obj):
    compare_obj = timedelta(days=3, hours=4)
    assert timedelta_obj == compare_obj


def date_util(dateutil_obj):
    compare_obj = parse("Thu Sep 26 10:36:28 2019")
    assert dateutil_obj == compare_obj


def time_tester(time_obj):
    compare_obj = time()
    # assert time_obj == compare_obj
    print(time_obj)  # <module 'time' (built-in)>
    print(compare_obj)  # 00:00:00


def ids(uuid_obj):
    compare_obj = uuid4()
    assert len(str(uuid_obj)) == len(str(compare_obj))


def mr_random(random_obj):
    print(random_obj)
    # for i in compare_ls:
    # if i == random_obj:
    # print("The Random module is working correctly")
    # else:
    # raise Exception("The random module is not working correctly")


# this function tests the airflow specific macros like ds_add() and ds_format()
def airflow_specific_macros(diff, d_add, d_form, rand):
    print(diff)
    assert diff == "2 days after"
    print(d_add)
    assert d_add == "2021-10-05"
    print(d_form)
    assert d_form == "11-12-2021"
    print(rand)
    assert rand > 0 and 1 > rand


with DAG(
    dag_id="builtin_macros_UNDRY",
    schedule=None,
    start_date=today("UTC").add(days=-2),
    render_template_as_native_obj=True,  # render templates using Jinja NativeEnvironment
    doc_md=docs,
    tags=["core"],
) as dag:
    py0 = PythonOperator(
        task_id="test_datetime_macros",
        python_callable=date_time,
        # yr, month, day, hr, min, sec
        op_args=["{{ macros.datetime(2021, 12, 12, 8, 32, 23) }}"],
    )

    py1 = PythonOperator(
        task_id="test_timedelta_macro_v1",
        python_callable=time_delta,
        op_args=["{{ macros.timedelta(days=3, hours=4) }}"],
    )

    py2 = PythonOperator(
        task_id="test_dateutil_package",
        python_callable=date_util,
        op_args=["{{ macros.dateutil.parser.parse('Thu Sep 26 10:36:28 2019') }}"],
    )

    py3 = PythonOperator(
        task_id="test_time_macros",
        python_callable=time_tester,
        # hrs, mins, secs
        # this fails
        op_args=["{{ macros.time(12, 32, 29) }}"]
        # this doesn't but gives a weird response
        # op_args=['{{ macros.time }}']
    )

    py4 = PythonOperator(
        task_id="test_uuid_macros",
        python_callable=ids,
        # generates random uuid
        op_args=["{{ macros.uuid.uuid4() }}"],
        trigger_rule="one_failed",
    )

    py5 = PythonOperator(
        task_id="test_random_macros",
        python_callable=mr_random,
        op_args=["{{ macros.random.choice(['docker-engine', 'docker-compose', 'docker_swarm']) }}"]
        # op_args=["{{ macros.random }}"]
    )

    py6 = PythonOperator(
        task_id="test_airflow_diff_specific_macros",
        python_callable=airflow_specific_macros,
        op_args=[
            datetime_diff_for_humans(today("UTC").add(days=-2), today("UTC").add(days=-4)),
            ds_add("2021-11-10", -36),
            ds_format("2021-12-11", "%Y-%d-%m", "%m-%d-%Y"),
            random(),
        ],
        trigger_rule="one_failed",
    )


py0 >> py1 >> py2 >> py3 >> py4 >> py5 >> py6
