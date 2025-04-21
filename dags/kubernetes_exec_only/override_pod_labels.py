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
"""
This is an example dag for using a Kubernetes Executor Configuration.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from airflow.sdk import DAG

log = logging.getLogger(__name__)

worker_container_repository = conf.get("kubernetes_executor", "worker_container_repository")
worker_container_tag = conf.get("kubernetes_executor", "worker_container_tag")

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning(
        "The example_kubernetes_executor example DAG requires the kubernetes provider."
        " Please install it with: pip install apache-airflow[cncf.kubernetes]"
    )
    k8s = None

# Dag for overriding pod labels, namespace, service account
if k8s:
    with DAG(
        dag_id="example_pod_override_labels",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["k8s_exe"],
    ) as dag:
        # You can use annotations on your kubernetes pods!
        start_task_executor_config = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
        }

        @task(executor_config=start_task_executor_config)
        def start_task():
            print_stuff()

        # [END task_with_sidecar]

        # You can add labels to pods
        executor_config_non_root = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"astronomer.io/executor": "CeleryExecutor"}))
        }

        @task(executor_config=executor_config_non_root)
        def non_root_task():
            print_stuff()

        other_executor_task = non_root_task()

        executor_config_other_ns = {"pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default"))}

        @task(executor_config=executor_config_other_ns)
        def other_namespace_task():
            print_stuff()

        other_ns_task = other_namespace_task()

        executor_config_service_account = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                    service_account="testing",
                )
            )
        }

        @task(executor_config=executor_config_service_account)
        def other_service_account_task():
            print_stuff()

        other_sa_task = other_service_account_task()

        # You can add labels to pods
        executor_config_non_root = {"pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"}))}

        @task(executor_config=executor_config_non_root)
        def non_root_task():
            print_stuff()

        add_label_task = non_root_task()
        (start_task() >> other_executor_task >> other_ns_task >> other_sa_task >> add_label_task)
