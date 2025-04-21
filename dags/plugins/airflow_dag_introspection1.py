import re
import time
from textwrap import indent

from airflow import settings
from airflow.configuration import conf
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.exceptions import AirflowRuntimeError
from airflow.utils.dot_renderer import render_dag
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import create_session
from elasticsearch import Elasticsearch
from sqlalchemy.orm import joinedload


def get_logs_chunks(log_id):
    es_host = conf.get("elasticsearch", "host")
    url_components = re.split(":|//|@", es_host)
    es = Elasticsearch(
        [f"http://{str(url_components[4])}:{str(url_components[5])}"],
        http_auth=(str(url_components[2]), str(url_components[3])),
    )
    query = {
        "bool": {"filter": [{"bool": {"should": [{"match_phrase": {"log_id": log_id}}], "minimum_should_match": 1}}]}
    }
    resp = es.search(
        index=f"fluentd.{str(url_components[2])}.*",
        body={"_source": ["message"], "query": query, "size": 10000},
        scroll="10m",
    )
    old_scroll_id = resp["_scroll_id"]
    results = resp["hits"]["hits"]
    result_log = []
    while len(results):
        for i in results:
            result_log.append(i["_source"]["message"])
        result = es.scroll(scroll_id=old_scroll_id, scroll="10m")  # length of time to keep search context
        # check if there's a new scroll ID
        if old_scroll_id != result["_scroll_id"]:
            print("NEW SCROLL ID:", result["_scroll_id"])
        # keep track of pass scroll _id
        old_scroll_id = result["_scroll_id"]
        results = result["hits"]["hits"]
    return "\n".join(result_log)


def log_checker(ti_id: str, expected: str, notexpected: str, try_number: int = 1, **context: dict):
    time.sleep(30)
    dagrun = context["dag_run"]
    task_instances = dagrun.get_task_instances()
    this_task_instance = next(filter(lambda ti: ti.task_id == ti_id, task_instances))  # ti_call
    dag_id = context["dag"].dag_id
    task_id = this_task_instance.task_id
    run_id = context["dag_run"].run_id
    map_index = -1
    log_id = f"{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}"

    def check(ti, expect, notexpect):
        remote_logging = conf.getboolean("logging", "remote_logging")
        logging_store_uri = conf.get("logging", "remote_base_log_folder")

        if remote_logging is True and logging_store_uri == "":
            logs = get_logs_chunks(log_id)
        else:
            with create_session() as session:
                ti = (
                    session.query(TaskInstance)
                    .filter(
                        TaskInstance.task_id == ti.task_id,
                        TaskInstance.dag_id == ti.dag_id,
                        TaskInstance.run_id == ti.run_id,
                        TaskInstance.map_index == ti.map_index,
                    )
                    .join(TaskInstance.dag_run)
                    .options(joinedload("trigger"))
                    .options(joinedload("trigger.triggerer_job"))
                ).first()
            task_log_reader = TaskLogReader()

            log_container, _ = task_log_reader.read_log_chunks(ti, try_number, {"download_logs": True})
            logs = log_container[0][0][1]
        print(f"Found logs: '''{logs}'''")
        assert notexpect not in logs
        assert expect in logs
        print(f"Found '''{expect}''' but not '''{notexpect}'''")

    # make sure expected output appeared
    check(this_task_instance, expected, notexpected)


def assert_homomorphic(task_group_names, **context):
    """
    The structure of all of the task groups above should be the same
    """
    # get the dag in dot notation, focus only on its edges
    dag = context["dag"]
    print(dag)
    # gives string which represents whole dag structure
    graph = render_dag(dag)
    print("Whole DAG:")
    print(indent(str(graph), "    "))
    lines = list(filter(lambda x: "->" in x, str(graph).split("\n")))

    # bin them by task group, then remove the group names
    group_strings = []
    # removes everything thats not a task name
    for name in task_group_names:
        print(name)
        relevant_lines = filter(lambda x: name in x, lines)
        normalized_lines = (x.strip().replace(name, "") for x in sorted(relevant_lines))
        edges_str = "\n".join(normalized_lines)
        group_strings.append(edges_str)
        print(indent(edges_str, "    "))

    # these should be identical
    for xgroup, ygroup in zip(group_strings, group_strings[1:]):
        assert xgroup == ygroup


def get_the_task_states(task_ids: list[str, str, str], **context) -> dict[str, str]:
    # This function returns a dictionary of task_ids and a tasks state from a list of task_ids
    dag_instance = context["dag"]
    logical_date = context["logical_date"]

    ls_of_statuses = []
    for i in task_ids:
        j = TaskInstance(dag_instance.get_task(i), execution_date=logical_date).current_state()
        ls_of_statuses.append(j)

    for i, j in zip(task_ids, ls_of_statuses):
        print(f"The state for the task with task_id: '{i}' is state: '{j}'")

    dict_of_results = {task_ids[i]: ls_of_statuses[i] for i in range(len(task_ids))}
    return dict_of_results


def assert_the_task_states(task_ids_and_assertions: dict[str, str], **context):
    # This function makes an assertion that supposed task states are actually that state
    # If the tasks are that state then it returns the value passed in, unaltered.
    dag_instance = context["dag"]
    logical_date = context["logical_date"]

    ls_of_statuses = []
    for i in task_ids_and_assertions.keys():
        j = TaskInstance(dag_instance.get_task(i), execution_date=logical_date).current_state()
        ls_of_statuses.append(j)

    for i, j, k in zip(task_ids_and_assertions.keys(), task_ids_and_assertions.values(), ls_of_statuses):
        print(f"The state for the task with task_id: '{i}' is state: '{k}'")
        assert j == k
    # By making an assert before the return statement the assert has to pass before a return statement is made
    # so as long as the states passed in are the same as the states generated from the TaskInstance class,
    # this return value will be correct even though it's the same unaltered value passed in.
    return task_ids_and_assertions


def add_conn(conn_id: str, conn_type: str, host: str, username: str, pw: str, scheme: str, port: int):
    try:
        BaseHook().get_connection(conn_id)
        print("The connection has been made previously.")
    except AirflowRuntimeError:
        remote_connection = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=username,
            password=pw,
            schema=scheme,
            port=port,
        )
        print(f"The connection that was created is: {remote_connection}")
        session = settings.Session()
        session.add(remote_connection)
        session.commit()


def delete_conn(connection_id):
    conn = Connection().get_connection_from_secrets(connection_id)
    session = settings.Session()
    session.delete(conn)
