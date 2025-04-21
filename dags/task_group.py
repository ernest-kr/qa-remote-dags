from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.utils.task_group import TaskGroup
from pendulum import today


def create_section():
    """
    Create tasks in the outer section.
    """
    dummies = [EmptyOperator(task_id=f"task-{i + 1}") for i in range(5)]

    with TaskGroup("inside_section_1") as inside_section_1:
        _ = [
            EmptyOperator(
                task_id=f"task-{i + 1}",
            )
            for i in range(3)
        ]

    with TaskGroup("inside_section_2") as inside_section_2:
        _ = [
            EmptyOperator(
                task_id=f"task-{i + 1}",
            )
            for i in range(3)
        ]

    dummies[-1] >> inside_section_1
    dummies[-2] >> inside_section_2


with DAG(dag_id="example_task_group", start_date=today("UTC").add(days=-1), tags=["core"]) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("section_1", tooltip="Tasks for Section 1") as section_1:
        create_section()

    some_other_task = EmptyOperator(task_id="some-other-task")

    with TaskGroup("section_2", tooltip="Tasks for Section 2") as section_2:
        create_section()

    end = EmptyOperator(task_id="end")

    start >> section_1 >> some_other_task >> section_2 >> end
