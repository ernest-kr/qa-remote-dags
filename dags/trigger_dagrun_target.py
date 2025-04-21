from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import today

dag = DAG(
    dag_id="trigger_target_dag",
    default_args={"start_date": today("UTC").add(days=-2), "owner": "Airflow"},
    tags=["core"],
    schedule=None,  # This must be none so it's triggered by the controller
    is_paused_upon_creation=False,  # This must be set so other workers can pick this dag up. mabye it's a bug idk
)


def run_this_func(**context):
    print(f"Remotely received value of {context['dag_run'].conf['message']} for key=message ")


run_this = PythonOperator(
    task_id="run_this",
    python_callable=run_this_func,
    dag=dag,
)

# You can also access the DagRun object in templates
bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: $message"',
    env={"message": '{{ dag_run.conf["message"] if dag_run else "" }}'},
    dag=dag,
)
