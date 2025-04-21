from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

# DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(
    dag_id="split_and_join",
    catchup=False,
    schedule=None,
    start_date=datetime(2017, 6, 23, 1, 0),
    end_date=None,
    tags=["core"],
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

a1 = EmptyOperator(task_id="a1", dag=dag)

start >> a1 >> end

b1 = EmptyOperator(task_id="b1", dag=dag)
b2 = EmptyOperator(task_id="b2", dag=dag)

start >> b1 >> b2 >> end

c1 = EmptyOperator(task_id="c1", dag=dag)
c2 = EmptyOperator(task_id="c2", dag=dag)
c3 = EmptyOperator(task_id="c3", dag=dag)

start >> c1 >> c2 >> c3 >> end

d1 = EmptyOperator(task_id="d1", dag=dag)
d2 = EmptyOperator(task_id="d2", dag=dag)
d3 = EmptyOperator(task_id="d3", dag=dag)
d4 = EmptyOperator(task_id="d4", dag=dag)

start >> d1 >> d2 >> d3 >> d4 >> end

e1 = EmptyOperator(task_id="e1", dag=dag)
e2 = EmptyOperator(task_id="e2", dag=dag)
e3 = EmptyOperator(task_id="e3", dag=dag)
e4 = EmptyOperator(task_id="e4", dag=dag)
e5 = EmptyOperator(task_id="e5", dag=dag)

start >> e1 >> e2 >> e3 >> e4 >> e5 >> end
