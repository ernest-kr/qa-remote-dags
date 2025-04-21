from datetime import datetime, timedelta
from math import ceil

import pause
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator


def wait_until_twenty_sec():
    now = datetime.now()
    extra = 20 - (ceil(now.second / 10) * 10)
    go_time = now + timedelta(seconds=extra)
    print(f"waiting until {go_time}")
    pause.until(go_time)


a7_a6 = Dataset("a7_a6")
a1_a3 = Dataset("a1_a3")
a4_a15 = Dataset("a4_a15")
a11_a1 = Dataset("a11_a1")
a1_a12 = Dataset("a1_a12")
a6_a1 = Dataset("a6_a1")
a5_a6 = Dataset("a5_a6")
a6_a2 = Dataset("a6_a2")
a6_a15 = Dataset("a6_a15")
a11_a2 = Dataset("a11_a2")
a7_a8 = Dataset("a7_a8")
a1_a2 = Dataset("a1_a2")
a9_a5 = Dataset("a9_a5")
a15_a14 = Dataset("a15_a14")
a13_a9 = Dataset("a13_a9")
a7_a14 = Dataset("a7_a14")
a4_a14 = Dataset("a4_a14")
a10_a11 = Dataset("a10_a11")
a10_a4 = Dataset("a10_a4")
a11_a8 = Dataset("a11_a8")
a5_a10 = Dataset("a5_a10")
a5_a12 = Dataset("a5_a12")
a15_a1 = Dataset("a15_a1")
a2_a7 = Dataset("a2_a7")
a14_a8 = Dataset("a14_a8")
a13_a5 = Dataset("a13_a5")
a15_a12 = Dataset("a15_a12")
a5_a14 = Dataset("a5_a14")
a14_a12 = Dataset("a14_a12")
a6_a4 = Dataset("a6_a4")
a14_a9 = Dataset("a14_a9")
a15_a9 = Dataset("a15_a9")

with DAG(
    dag_id="a1",
    start_date=datetime(1970, 1, 1),
    schedule=[a6_a1, a11_a1, a15_a1],
) as a1:
    PythonOperator(task_id="a1_a2", python_callable=wait_until_twenty_sec, outlets=[a1_a2])
    PythonOperator(task_id="a1_a3", python_callable=wait_until_twenty_sec, outlets=[a1_a3])
    PythonOperator(task_id="a1_a12", python_callable=wait_until_twenty_sec, outlets=[a1_a12])

with DAG(
    dag_id="a2",
    start_date=datetime(1970, 1, 1),
    schedule=[a1_a2, a6_a2, a11_a2],
) as a2:
    PythonOperator(task_id="a2_a7", python_callable=wait_until_twenty_sec, outlets=[a2_a7])

with DAG(
    dag_id="a3",
    start_date=datetime(1970, 1, 1),
    schedule=[a1_a3],
) as a3:
    PythonOperator(task_id="no_outlets", python_callable=wait_until_twenty_sec)

with DAG(
    dag_id="a6",
    start_date=datetime(1970, 1, 1),
    schedule=[a5_a6, a7_a6],
) as a6:
    PythonOperator(task_id="a6_a1", python_callable=wait_until_twenty_sec, outlets=[a6_a1])
    PythonOperator(task_id="a6_a2", python_callable=wait_until_twenty_sec, outlets=[a6_a2])
    PythonOperator(task_id="a6_a4", python_callable=wait_until_twenty_sec, outlets=[a6_a4])
    PythonOperator(task_id="a6_a15", python_callable=wait_until_twenty_sec, outlets=[a6_a15])

with DAG(
    dag_id="a11",
    start_date=datetime(1970, 1, 1),
    schedule=[a10_a11],
) as a11:
    PythonOperator(task_id="a11_a1", python_callable=wait_until_twenty_sec, outlets=[a11_a1])
    PythonOperator(task_id="a11_a2", python_callable=wait_until_twenty_sec, outlets=[a11_a2])
    PythonOperator(task_id="a11_a8", python_callable=wait_until_twenty_sec, outlets=[a11_a8])

with DAG(
    dag_id="a12",
    start_date=datetime(1970, 1, 1),
    schedule=[a1_a12, a5_a12, a14_a12, a15_a12],
) as a12:
    PythonOperator(task_id="no_outlets", python_callable=wait_until_twenty_sec)

with DAG(
    dag_id="a15",
    start_date=datetime(1970, 1, 1),
    schedule=[a4_a15, a6_a15],
) as a15:
    PythonOperator(task_id="a15_a1", python_callable=wait_until_twenty_sec, outlets=[a15_a1])
    PythonOperator(task_id="a15_a9", python_callable=wait_until_twenty_sec, outlets=[a15_a9])
    PythonOperator(task_id="a15_a12", python_callable=wait_until_twenty_sec, outlets=[a15_a12])
    PythonOperator(task_id="a15_a14", python_callable=wait_until_twenty_sec, outlets=[a15_a14])

with DAG(
    dag_id="a7",
    start_date=datetime(1970, 1, 1),
    schedule=[a2_a7],
) as a7:
    PythonOperator(task_id="a7_a6", python_callable=wait_until_twenty_sec, outlets=[a7_a6])
    PythonOperator(task_id="a7_a8", python_callable=wait_until_twenty_sec, outlets=[a7_a8])
    PythonOperator(task_id="a7_a14", python_callable=wait_until_twenty_sec, outlets=[a7_a14])

with DAG(
    dag_id="a4",
    start_date=datetime(1970, 1, 1),
    schedule=[a6_a4, a10_a4],
) as a4:
    PythonOperator(task_id="a4_a14", python_callable=wait_until_twenty_sec, outlets=[a4_a14])
    PythonOperator(task_id="a4_a15", python_callable=wait_until_twenty_sec, outlets=[a4_a15])

with DAG(
    dag_id="a10",
    start_date=datetime(1970, 1, 1),
    schedule=[a5_a10],
) as a10:
    PythonOperator(task_id="a10_a4", python_callable=wait_until_twenty_sec, outlets=[a10_a4])
    PythonOperator(task_id="a10_a11", python_callable=wait_until_twenty_sec, outlets=[a10_a11])

with DAG(
    dag_id="a14",
    start_date=datetime(1970, 1, 1),
    schedule=[a4_a14, a5_a14, a7_a14, a15_a14],
) as a14:
    PythonOperator(task_id="a14_a8", python_callable=wait_until_twenty_sec, outlets=[a14_a8])
    PythonOperator(task_id="a14_a9", python_callable=wait_until_twenty_sec, outlets=[a14_a9])
    PythonOperator(task_id="a14_a12", python_callable=wait_until_twenty_sec, outlets=[a14_a12])

with DAG(
    dag_id="a5",
    start_date=datetime(1970, 1, 1),
    schedule=[a9_a5, a13_a5],
) as a5:
    PythonOperator(task_id="a5_a6", python_callable=wait_until_twenty_sec, outlets=[a5_a6])
    PythonOperator(task_id="a5_a10", python_callable=wait_until_twenty_sec, outlets=[a5_a10])
    PythonOperator(task_id="a5_a12", python_callable=wait_until_twenty_sec, outlets=[a5_a12])
    PythonOperator(task_id="a5_a14", python_callable=wait_until_twenty_sec, outlets=[a5_a14])

with DAG(
    dag_id="a9",
    start_date=datetime(1970, 1, 1),
    schedule=[a13_a9, a14_a9, a15_a9],
) as a9:
    PythonOperator(task_id="a9_a5", python_callable=wait_until_twenty_sec, outlets=[a9_a5])

with DAG(
    dag_id="start_a13",
    start_date=datetime(1970, 1, 1),
    schedule=None,
) as start_a13:
    PythonOperator(task_id="a13_a5", python_callable=wait_until_twenty_sec, outlets=[a13_a5])
    PythonOperator(task_id="a13_a9", python_callable=wait_until_twenty_sec, outlets=[a13_a9])

with DAG(
    dag_id="a8",
    start_date=datetime(1970, 1, 1),
    schedule=[a7_a8, a11_a8, a14_a8],
) as a8:
    PythonOperator(task_id="no_outlets", python_callable=wait_until_twenty_sec)
