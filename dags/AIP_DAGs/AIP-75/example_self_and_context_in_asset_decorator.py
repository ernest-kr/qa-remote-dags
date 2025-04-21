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
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.sdk.definitions.asset.decorators import asset


@asset(uri="s3://bucket/asset1_producer", schedule=None, tags=["asset", "AIP-75"])
def asset1_producer():
    pass


@asset(name="asset_target", schedule=asset1_producer, tags=["asset", "AIP-75"])
def asset_target():
    pass


@asset(uri="s3://bucket/object", schedule=None, tags=["asset", "AIP-75"])
def asset2_producer(self, context, asset1_producer):
    print(self)
    print(context["inlet_events"][asset1_producer])


@dag(
    schedule=(asset1_producer | Asset(uri="s3://bucket/asset3_producer", name="asset3_producer")),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["asset", "AIP-75"],
)
def consumes_asset_decorator():
    @task(outlets=[asset1_producer])
    def process_nothing():
        pass

    process_nothing()


consumes_asset_decorator()
