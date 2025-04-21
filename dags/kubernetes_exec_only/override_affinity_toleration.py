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


if k8s:
    with DAG(
        dag_id="example_tolerations_override",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["k8s_exe"],
    ) as dag:
        tolerations = [{"key": "dedicated", "operator": "Equal", "value": "airflow"}]

        executor_config_tolerations = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            # volume_mounts=[k8s.V1VolumeMount(mount_path="/shared/", name="shared-empty-dir")],
                        ),
                    ],
                    tolerations=tolerations,
                )
            ),
        }

        @task(executor_config=executor_config_tolerations)
        def other_toleration_task():
            print_stuff()

        other_tl_task = other_toleration_task()
        other_tl_task

    with DAG(
        dag_id="example_pod_affinity_override",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["k8s_exe"],
    ) as dag:
        affinity = k8s.V1PodAffinity(
            required_during_scheduling_ignored_during_execution=[
                k8s.V1PodAffinityTerm(
                    topology_key="kubernetes.io/hostname",
                    label_selector=k8s.V1LabelSelector(
                        match_expressions=[k8s.V1LabelSelectorRequirement(key="app", operator="in", values=["airflow"])]
                    ),
                )
            ]
        )
        executor_config_affinity = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            # volume_mounts=[k8s.V1VolumeMount(mount_path="/shared/", name="shared-empty-dir")],
                        ),
                    ],
                    affinity=affinity,
                )
            ),
        }

        @task(executor_config=executor_config_affinity)
        def other_affinity_task():
            print_stuff()

        other_af_task = other_affinity_task()

        other_af_task
