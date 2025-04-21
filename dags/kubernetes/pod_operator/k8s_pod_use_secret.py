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


from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperator,
)

# from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sdk import DAG
from pendulum import today

namespace = conf.get("kubernetes_executor", "NAMESPACE")

# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace == "default":
    config_file = "/usr/local/airflow/include/.kube/config"
    in_cluster = False
else:
    in_cluster = True
    config_file = None

secret_file = Secret("volume", "/etc/sql_conn", "airflow-secrets-1", "sql_alchemy_conn")
secret_env = Secret("env", "SQL_CONN", "airflow-secrets-2", "sql_alchemy_conn")
secret_all_keys = Secret("env", None, "airflow-secrets-2")

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="k8_pod_use_secret",
    default_args=default_args,
    schedule=None,
    start_date=today("UTC").add(days=-2),
    tags=["kpo"],
) as dag:
    k = KubernetesPodOperator(
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
        image="ubuntu:latest",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        secrets=[secret_file, secret_env, secret_all_keys],
        name="airflow-use-secret-pod",
        task_id="task-with-secret",
        is_delete_operator_pod=False,
    )
