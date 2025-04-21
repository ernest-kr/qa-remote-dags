#
# Licensed to the Apache Software Foundation (asF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The asF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "as IS" BasIS, withOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import setup, task, task_group, teardown
from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG

#############################
# these ARE passing

with DAG(
    dag_id="example_setup_one_to_many_as_teardown",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    1 setup mapping to 3 teardowns
    1 work task
    work fails
    teardowns succeed
    dagrun should be failure
    """

    @task
    def my_setup():
        print("setting up multiple things")
        return [1, 2, 3]

    @task
    def my_work(val):
        print(f"doing work with multiple things: {val}")
        raise ValueError("this fails")
        return val

    @task
    def my_teardown(val):
        print(f"teardown: {val}")

    s = my_setup()
    t = my_teardown.expand(val=s).as_teardown(setups=s)
    with t:
        my_work(s)

with DAG(
    dag_id="example_setup_one_to_many_as_teardown_offd",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown"],
) as dag:
    """-- passing
    1 setup mapping to 3 teardowns
    1 work task
    work succeeds
    all but one teardown succeed
    offd=True
    dagrun should be success
    """

    @task
    def my_setup():
        print("setting up multiple things")
        return [1, 2, 3]

    @task
    def my_work(val):
        print(f"doing work with multiple things: {val}")
        return val

    @task
    def my_teardown(val):
        print(f"teardown: {val}")
        if val == 2:
            raise ValueError("failure")

    s = my_setup()
    t = my_teardown.expand(val=s).as_teardown(setups=s, on_failure_fail_dagrun=True)
    with t:
        my_work(s)
    # todo: if on_failure_fail_dagrun=True, should we still regard the WORK task as a leaf?

with DAG(
    dag_id="example_mapped_task_group_simple",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    Mapped task group wherein there's a simple s >> w >> t pipeline.
    When s is skipped, all should be skipped
    When s is failed, all should be upstream failed
    """

    @setup
    def my_setup(val):
        if val == "data2.json":
            raise ValueError("fail!")
        elif val == "data3.json":
            raise AirflowSkipException("skip!")
        print(f"setup: {val}")

    @task
    def my_work(val):
        print(f"work: {val}")

    @teardown
    def my_teardown(val):
        print(f"teardown: {val}")

    @task_group
    def file_transforms(filename):
        s = my_setup(filename)
        t = my_teardown(filename).as_teardown(setups=s)
        with t:
            my_work(filename)

    file_transforms.expand(filename=["data1.json", "data2.json", "data3.json"])

with DAG(
    dag_id="example_mapped_task_group_work_fail_or_skip",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    Mapped task group wherein there's a simple s >> w >> t pipeline.
    When w is skipped, teardown should still run
    When w is failed, teardown should still run
    """

    @setup
    def my_setup(val):
        print(f"setup: {val}")

    @task
    def my_work(val):
        if val == "data2.json":
            raise ValueError("fail!")
        elif val == "data3.json":
            raise AirflowSkipException("skip!")
        print(f"work: {val}")

    @teardown
    def my_teardown(val):
        print(f"teardown: {val}")

    @task_group
    def file_transforms(filename):
        s = my_setup(filename)
        t = my_teardown(filename).as_teardown(setups=s)
        with t:
            my_work(filename)

    file_transforms.expand(filename=["data1.json", "data2.json", "data3.json"])

with DAG(
    dag_id="example_setup_teardown_many_one_explicit",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    one mapped setup going to one unmapped work
    3 diff states for setup: success / failed / skipped
    teardown still runs, and receives the xcom from the single successful setup
    """

    @task
    def my_setup(val):
        if val == "data2.json":
            raise ValueError("fail!")
        elif val == "data3.json":
            raise AirflowSkipException("skip!")
        print(f"setup: {val}")
        return val

    @task
    def my_work(val):
        print(f"work: {val}")

    @task
    def my_teardown(val):
        print(f"teardown: {val}")

    s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
    with my_teardown(s).as_teardown(setups=s):
        w = my_work(s)


with DAG(
    dag_id="example_setup_teardown_many_one_explicit_odd_setup_one_mapped_fails",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    one unmapped setup goes to two different teardowns
    one mapped setup goes to same teardown
    one of the mapped setup instances fails
    teardowns should all run
    """

    @task
    def other_setup():
        print("other setup")
        return "other setup"

    @task
    def other_work():
        print("other work")
        return "other work"

    @task
    def other_teardown():
        print("other teardown")
        return "other teardown"

    @task
    def my_setup(val):
        if val == "data2.json":
            raise ValueError("fail!")
        elif val == "data3.json":
            raise AirflowSkipException("skip!")
        print(f"setup: {val}")
        return val

    @task
    def my_work(val):
        print(f"work: {val}")

    @task
    def my_teardown(val):
        print(f"teardown: {val}")

    s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
    o_setup = other_setup()
    o_teardown = other_teardown()
    with o_teardown.as_teardown(setups=o_setup):
        o_work = other_work()
    t = my_teardown(s).as_teardown(setups=s)
    with t:
        w = my_work(s)
    o_setup >> t


with DAG(
    dag_id="example_setup_teardown_many_one_explicit_odd_setup_all_setups_fail",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    one unmapped setup goes to two different teardowns
    one mapped setup goes to same teardown
    all setups fail
    teardowns should not run
    """

    @task
    def other_setup():
        print("other setup")
        raise ValueError("fail")
        return "other setup"

    @task
    def other_work():
        print("other work")
        return "other work"

    @task
    def other_teardown():
        print("other teardown")
        return "other teardown"

    @task
    def my_setup(val):
        print(f"setup: {val}")
        raise ValueError("fail")
        return val

    @task
    def my_work(val):
        print(f"work: {val}")

    @task
    def my_teardown(val):
        print(f"teardown: {val}")

    s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
    o_setup = other_setup()
    o_teardown = other_teardown()
    with o_teardown.as_teardown(setups=o_setup):
        o_work = other_work()
    t = my_teardown(s).as_teardown(setups=s)
    with t:
        w = my_work(s)
    o_setup >> t

with DAG(
    dag_id="example_setup_teardown_many_one_explicit_odd_setup_mapped_setups_fail",
    start_date=datetime(2022, 3, 4),
    catchup=False,
    tags=["setup_teardown_neg"],
) as dag:
    """-- passing
    one unmapped setup goes to two different teardowns
    one mapped setup goes to same teardown
    mapped setups fail
    teardowns should still run
    """

    @task
    def other_setup():
        print("other setup")
        return "other setup"

    @task
    def other_work():
        print("other work")
        return "other work"

    @task
    def other_teardown():
        print("other teardown")
        return "other teardown"

    @task
    def my_setup(val):
        print(f"setup: {val}")
        raise ValueError("fail")
        return val

    @task
    def my_work(val):
        print(f"work: {val}")

    @task
    def my_teardown(val):
        print(f"teardown: {val}")

    s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
    o_setup = other_setup()
    o_teardown = other_teardown()
    with o_teardown.as_teardown(setups=o_setup):
        o_work = other_work()
    t = my_teardown(s).as_teardown(setups=s)
    with t:
        w = my_work(s)
    o_setup >> t

with DAG(
    dag_id="example_setup_one_to_many", start_date=datetime(2022, 3, 4), catchup=False, tags=["setup_teardown_neg"]
) as dag:
    """one to many should also work with the decorator defs"""

    @setup
    def my_setup():
        print("setting up multiple things")
        return [1, 2, 3]

    @task
    def my_work(val):
        print(f"doing work with multiple things: {val}")
        raise ValueError("fail!")
        return val

    @teardown
    def my_teardown(val):
        print(f"teardown: {val}")

    s = my_setup()
    t = my_teardown.expand(val=s)
    with t:
        w = my_work(s)
