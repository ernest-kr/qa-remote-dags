from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from pendulum import today

docs = """
####To Test:
Check in the Security tab of the airflow webserver UI under 'List Roles' to ensure dag permissions show up in the access level.

####Context
'access_control' allows the user of airflow to create custom views which dictates what the user can view.\n
Airflow treats the User role as the template for all custom role.\n
More info can be found here: https://airflow.apache.org/docs/apache-airflow/stable/security/access-control.html#user \n
####Purpose
This dag tests the dag parameter 'access_control' to ensure you can create custom roles.
"""

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": today("UTC").add(days=-1),
}

dag = DAG(
    dag_id="access_control",
    default_args=DEFAULT_ARGS,
    schedule=None,
    # check that the values in this dictionary show up
    access_control={"qa-team": {"can_dag_read", "can_dag_edit"}},
    doc_md=docs,
    tags=["dagparams"],
)

t1 = BashOperator(task_id="should_pass", bash_command="date", dag=dag)
