from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG("Scheduler_OOM_test", schedule=None, tags=["scheduler_tests"]):
    start = EmptyOperator(task_id="start")

    a = [
        start
        >> EmptyOperator(task_id=f"a_1_{i}")
        >> EmptyOperator(task_id=f"a_2_{i}")
        >> EmptyOperator(task_id=f"a_3_{i}")
        for i in range(200)
    ]

    middle = EmptyOperator(task_id="middle")

    b = [
        middle
        >> EmptyOperator(task_id=f"b_1_{i}")
        >> EmptyOperator(task_id=f"b_2_{i}")
        >> EmptyOperator(task_id=f"b_3_{i}")
        for i in range(200)
    ]

    middle2 = EmptyOperator(task_id="middle2")

    c = [
        middle2
        >> EmptyOperator(task_id=f"c_1_{i}")
        >> EmptyOperator(task_id=f"c_2_{i}")
        >> EmptyOperator(task_id=f"c_3_{i}")
        for i in range(200)
    ]

    end = EmptyOperator(task_id="end")

    start >> a >> middle >> b >> middle2 >> c >> end
