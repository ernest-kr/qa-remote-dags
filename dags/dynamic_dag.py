from airflow.decorators import dag, task
from pendulum import today


def create_dag(dag_id):
    @dag(
        default_args={"owner": "airflow", "start_date": today("UTC").add(days=-1)},
        schedule=None,
        dag_id=dag_id,
        catchup=False,
        is_paused_upon_creation=False,
        tags=["core"],
    )
    def dynamic_dag():
        @task()
        def dynamic_task_1(*name):
            return 1

        task_1 = dynamic_task_1()
        dynamic_task_1(task_1)

    return dynamic_dag()


for i in range(3):
    dag_id = f"dynamic_dag_{i}"
    globals()[dag_id] = create_dag(dag_id)
