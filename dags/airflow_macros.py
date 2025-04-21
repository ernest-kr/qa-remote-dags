import datetime as dt
import time
from uuid import uuid4

from airflow.macros import datetime_diff_for_humans, ds_add, ds_format, random as random2
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dateutil.parser import parse as dateutil_parse
from pendulum import today

docs = """
####Purpose
This dag tests that all of the predefined macros work as they should.\n
Currently to use macros.time and macros.random a workaround is used as the docs don't properly describe how to use them.\n
The docs describe these macros as either the random module for macros.random or the datetime.time class when in fact the random macro generated a random value between 0 and 1 macros.time is the time module not datetime.time as the docs say.
####Expected Behavior
This dag has 20 tasks all of which are expected to succceed. If the tasks 'showdoc_time' and 'test_time' fail it may be because of a bug fix.\n
Likewise if the tasks 'showdoc_random' and 'test_random' fail it may also be because of a bug fix.
"""


"""
According to the docs:

    macros.datetime - datetime.datetime
    macros.timedelta - datetime.timedelta
    macros.datetutil - dateutil package
    macros.time - datetime.time
    macros.uuid - python standard lib uuid
    macros.random - python standard lib random

According to the code:

    macros.datetime - datetime.datetime
    macros.timedelta - datetime.timedelta
    macros.datetutil - dateutil package
    macros.time - python standard lib time  <--- differs
    macros.uuid - python standard lib uuid
    macros.random - random.random           <--- differs
"""


def date_time(datetime_obj):
    compare_obj = dt.datetime(2021, 12, 12, 8, 32, 23)
    assert datetime_obj == compare_obj


def time_delta(timedelta_obj):
    compare_obj = dt.timedelta(days=3, hours=4)
    assert timedelta_obj == compare_obj


def date_util(dateutil_obj):
    compare_obj = dateutil_parse("Thu Sep 26 10:36:28 2019")
    assert dateutil_obj == compare_obj


def time_tester(time_obj):
    # note that datetime.time.time() gives an AttributeError
    # time.time() on the other hand, returns a float

    # this works because macro.time isn't 'datetime.time', like the docs say
    # it's just 'time'
    compare_obj = time.time()
    print(time_obj)
    print(compare_obj)

    # the macro might have captured a slightly differnt time than the task,
    # but they're not going to be more than 10s apart
    assert abs(time_obj - compare_obj) < 10


def uuid_tester(uuid_obj):
    compare_obj = uuid4()
    assert len(str(uuid_obj)) == len(str(compare_obj))


def random_tester(random_float):
    # note that 'random.random' is a function that returns a float
    # while 'random' is a module (and isn't callable)

    # the macro was 'macros.random()' and here we have a float:
    assert -0.1 < random_float < 100.1

    # so the docs are wrong here too
    # macros.random actually returns a function, not the random module


def diff_for_humans(diff):
    assert diff == "2 days after"


def start_date_add(d_add):
    assert d_add == "2021-10-05"


def date_formatter(d_form):
    assert d_form == "11-12-2021"


def rand_macro(rand):
    assert rand > 0 and 1 > rand


def show_docs(attr):
    print(attr.__doc__)


with DAG(
    dag_id="builtin_macros_most_recent",
    schedule=None,
    start_date=dt.datetime(1970, 1, 1),
    render_template_as_native_obj=True,  # render templates using Jinja NativeEnvironment
    doc_md=docs,
    tags=["core"],
) as dag:
    test_functions = {
        "datetime": (date_time, "{{ macros.datetime(2021, 12, 12, 8, 32, 23) }}"),
        "timedelta": (time_delta, "{{ macros.timedelta(days=3, hours=4) }}"),
        "dateutil": (
            date_util,
            "{{ macros.dateutil.parser.parse('Thu Sep 26 10:36:28 2019') }}",
        ),
        "time": (time_tester, "{{  macros.time.time() }}"),
        "uuid": (uuid_tester, "{{ macros.uuid.uuid4() }}"),
        "random": (
            random_tester,
            "{{ 100 * macros.random() }}",
        ),
        "datetime_diff_for_humans": (
            diff_for_humans,
            datetime_diff_for_humans(today("UTC").add(days=-2), today("UTC").add(days=-4)),
        ),
        "ds_add": (start_date_add, ds_add("2021-11-10", -36)),
        "ds_format": (date_formatter, ds_format("2021-12-11", "%Y-%d-%m", "%m-%d-%Y")),
        "random2": (rand_macro, random2()),
    }
    for name, (func, template) in test_functions.items():
        (
            PythonOperator(
                task_id=f"showdoc_{name}",
                python_callable=show_docs,
                op_args=[f"{{{{ macros.{name} }}}}"],
            )
            >> PythonOperator(task_id=f"test_{name}", python_callable=func, op_args=[template])
        )
