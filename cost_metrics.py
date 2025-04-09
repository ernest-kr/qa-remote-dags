import os
import datetime
import requests

from pprint import pprint

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

ENV = os.environ["ENV"]
API_ORIGIN = (
    "https://api.astronomer.io" if ENV == "prod" else f"https://api.astronomer-{ENV}.io"
)
ORG_ID = os.environ["ASTRO_ORGANIZATION_ID"]
API_ORG_URL = f"{API_ORIGIN}/private/v1alpha1/organizations/{ORG_ID}"
CLIENT_ID = "postman-core-workspace"


@task(multiple_outputs=True)
def get_query_ids(data_interval_start, data_interval_end, ti, var):
    # Format to rfc3339 because that's what the API accepts.

    # Adding a lookback period of 6 hours. The QUERY_ATTRIBUTION_HISTORY view can have a lag of up to 6 hours.
    # Accounting for that entire, possible lag time here.
    # See https://docs.snowflake.com/en/sql-reference/account-usage/query_attribution_history#usage-notes.
    start = data_interval_start.subtract(hours=6).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    end = data_interval_end.subtract(hours=6).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    print(f"Getting queries executed from {start} to {end}")

    get_queries_url = f"{API_ORG_URL}/observability/external-queries?earliestTime={start}&latestTime={end}"

    print(f"Getting queries from {get_queries_url}")

    token = var["value"].AIRFLOW_VAR_AUTH_TOKEN
    resp = requests.get(
        get_queries_url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Astro-Client-Identifier": CLIENT_ID,
        },
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        raise Exception(
            f"Failed to get queries: {resp.status_code}:{resp.reason} {resp.text}"
        )

    queries = resp.json().get("externalQueries")
    print(f"Collected {len(queries)} queries.")

    # Store mapping for later when we need to post cost attribution
    query_run_mapping = {query["queryId"]: query for query in queries}

    return {
        "query_ids": list(query_run_mapping.keys()),
        "query_id_map": query_run_mapping,
    }


@task.short_circuit
def check_for_query_ids(this: list[str]) -> bool:
    """Short-circuits the pipeline if no query IDs were retrieved from get_query_ids task."""
    if not (res := bool(this)):
        print("No queries retrieved.")

    return res


@task(execution_timeout=datetime.timedelta(minutes=10))
def post_cost_attribution(query_costs, ti, var):
    if not query_costs:
        print("No costs to post")
        return

    query_run_mapping = ti.xcom_pull(key="query_id_map", task_ids="get_query_ids")

    costs = []
    for query_id, end_time, credit in query_costs:
        query_meta = query_run_mapping.get(query_id)
        costs.append(
            {
                "value": credit,
                "assetId": query_meta["assetId"],
                "deploymentId": query_meta["deploymentId"],
                "runId": query_meta["runId"],
                "dagId": query_meta["dagId"],
                "taskId": query_meta["taskId"],
                "namespace": query_meta["namespace"],
                # Turn the datetime into rfc3339
                "timestamp": end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
        )

    print(f"::group::Posting {len(costs)} cost items:")
    pprint(costs, indent=2)
    print("::endgroup::")

    token = var["value"].AIRFLOW_VAR_AUTH_TOKEN
    resp = requests.post(
        f"{API_ORG_URL}/observability/metrics",
        json={"category": "COST", "type": "SNOWFLAKE_CREDITS", "metrics": costs},
        headers={
            "Authorization": f"Bearer {token}",
            "X-Astro-Client-Identifier": CLIENT_ID,
        },
    )

    if resp.status_code != 200:
        raise Exception(f"Failed to post cost: {resp.text}")


@task(execution_timeout=datetime.timedelta(minutes=10))
def post_query_rows_processed(rows_processed, var, ti):
    if not rows_processed:
        print("No rows processed to post")
        return

    query_run_mapping = ti.xcom_pull(key="query_id_map", task_ids="get_query_ids")

    produced = []
    inserted = []
    updated = []
    deleted = []
    unloaded = []
    elapsed = []
    scanned = []
    for (
        query_id,
        rows_produced,
        rows_inserted,
        rows_updated,
        rows_deleted,
        rows_unloaded,
        total_elapsed_time,
        bytes_scanned,
        end_time,
    ) in rows_processed:
        query_meta = query_run_mapping.get(query_id)

        def make_row(value):
            value = value if value is not None else 0
            return {
                "value": value,
                "assetId": query_meta["assetId"],
                "deploymentId": query_meta["deploymentId"],
                "runId": query_meta["runId"],
                "dagId": query_meta["dagId"],
                "taskId": query_meta["taskId"],
                "namespace": query_meta["namespace"],
                "timestamp": end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }

        produced.append(make_row(rows_produced))
        inserted.append(make_row(rows_inserted))
        updated.append(make_row(rows_updated))
        deleted.append(make_row(rows_deleted))
        unloaded.append(make_row(rows_unloaded))
        elapsed.append(make_row(total_elapsed_time))
        scanned.append(make_row(bytes_scanned))

    token = var["value"].AIRFLOW_VAR_AUTH_TOKEN

    def make_request(data, type):
        print(f"::group::Posting {len(data)} {type} items:")
        pprint(data, indent=2)
        print("::endgroup::")
        resp = requests.post(
            f"{API_ORG_URL}/observability/metrics",
            json={"category": "CUSTOM", "type": f"SNOWFLAKE_{type}", "metrics": data},
            headers={
                "Authorization": f"Bearer {token}",
                "X-Astro-Client-Identifier": CLIENT_ID,
            },
        )
        if resp.status_code != 200:
            raise Exception(f"Failed to post data: {resp.text}")
        return resp.json()

    make_request(produced, "ROWS_PRODUCED")
    make_request(inserted, "ROWS_INSERTED")
    make_request(updated, "ROWS_UPDATED")
    make_request(deleted, "ROWS_DELETED")
    make_request(unloaded, "ROWS_UNLOADED")
    make_request(elapsed, "TOTAL_ELAPSED_TIME")
    make_request(scanned, "BYTES_SCANNED")


@dag(
    start_date=datetime.datetime(2024, 10, 1),
    schedule="@hourly",
    catchup=False,
    render_template_as_native_obj=True,
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=1),
        "show_return_value_in_logs": False,
    },
)
def cost_attribution():
    """
    Pulls Query IDs from the Astronomer API, then queries Snowflake's account_usage.query_attribution_history
    to get the credits attributed to each query. Finally, posts the costs to the Astronomer API.
    """

    get_queries = get_query_ids()
    check = check_for_query_ids(get_queries["query_ids"])

    cost_attribution = SQLExecuteQueryOperator(
        task_id="cost_attribution",
        conn_id="snowflake",
        sql="""
            select
                query_id,
                end_time,
                credits_attributed_compute
            from snowflake.account_usage.query_attribution_history
            where query_id in (%s)
        """,
        parameters=[get_queries["query_ids"]],
    )

    rows_processed_attribution = SQLExecuteQueryOperator(
        task_id="rows_processed_attribution",
        conn_id="snowflake",
        sql="""
            select
                query_id,
                rows_produced,
                rows_inserted,
                rows_updated,
                rows_deleted,
                rows_unloaded,
                total_elapsed_time,
                bytes_scanned,
                end_time
            from snowflake.account_usage.query_history
            where query_id in ( %s )
        """,
        parameters=[get_queries["query_ids"]],
    )

    check >> [cost_attribution, rows_processed_attribution]
    post_cost_attribution(query_costs=cost_attribution.output)
    post_query_rows_processed(rows_processed=rows_processed_attribution.output)


cost_attribution()