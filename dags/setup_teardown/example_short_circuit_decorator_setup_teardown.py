# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the `@task.short_circuit()` TaskFlow decorator."""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import chain
from airflow.utils.trigger_rule import TriggerRule


@dag(start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False, tags=["setup_teardown"])
def example_short_circuit_decorator_setup_teardown():
    @task.short_circuit()
    def check_condition(condition):
        return condition

    ds_true = [EmptyOperator(task_id="true_" + str(i)) for i in [1, 2]]
    ds_false = [EmptyOperator(task_id="false_" + str(i)) for i in [1, 2]]
    condition_is_true = check_condition.override(task_id="condition_is_true")(condition=True).as_setup()
    condition_is_false = check_condition.override(task_id="condition_is_false")(condition=False)
    t = EmptyOperator(task_id="teardown").as_teardown(setups=condition_is_true)
    t1 = EmptyOperator(task_id="teardown1", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS).as_teardown(
        setups=condition_is_false
    )
    chain(condition_is_true, *ds_true, t)
    chain(condition_is_false, *ds_false, t1)


example_dag = example_short_circuit_decorator_setup_teardown()
