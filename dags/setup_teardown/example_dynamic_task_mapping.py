#
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
"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import setup, task, teardown
from airflow.sdk import DAG

with DAG(
    dag_id="example_dynamic_task_mapping",
    start_date=datetime(2022, 3, 4),
    schedule=None,
    catchup=False,
    tags=["setup_teardown"],
) as dag:

    @setup
    def initial():
        print("setup")

    @task
    def add_one(x: int):
        return x + 1

    @teardown
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    s = initial()
    values = add_one.expand(x=[1, 2, 3])
    t = sum_it(values)

    with s >> t as context:
        context.add_task(values)
