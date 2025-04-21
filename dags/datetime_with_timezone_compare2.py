# This compares the datetime objects with timezones

# do something with time with both datetime and pendulum

# dags can do same thing

# give it a start date in the past

# proposed test plan:
# dag1:
#  - pendulum start time in the past
#  - standard datetime in the future as a user macro
#  - task that calculates the difference between the two and prints it
# dag2:
#  - standard start time in the past
#  - pendulum datetime in the future as a user macro
#  - task that calculates the difference between the two and prints it
# (future) outer dag:
# - run both and compare that they do the same things

# templated fields to calculate how long ago the start date is
import datetime

# from pendulum import datetime, timezone
import pendulum
import pytz
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

etc_gmt13 = "Etc/GMT-13"

chuuk = "Pacific/Chuuk"

# start_date = pendulum.datetime(2021, 11, 25, tz=pendulum.timezone('Pacific/Chuuk'))
start_date = pytz.timezone("Etc/GMT-13").localize(datetime.datetime(2021, 11, 25))


def subtractor(start_date, future_date):
    print(type(start_date), type(future_date))  # both strings, now find a way to compare
    if "T" in start_date or "T" in future_date:
        start_date = start_date.replace("T", " ")
        future_date = future_date.replace("T", " ")
    if "00:00:00" in start_date or "00:00:00" in future_date:
        start_date = start_date.replace("00:00:00", "")
        future_date = future_date.replace("00:00:00", "")

    print(start_date, future_date)  # 2021-11-25 00:00:00+10:00 2135-11-25 00:00:00+13:00

    for i in start_date:
        if "-" == i:
            start_date = start_date.replace("-", "_")

    for i in future_date:
        if "-" == i:
            future_date = future_date.replace("-", "_")

    print(f"The newly filtered start date is {start_date} and the newly filtered future date is {future_date}")
    # 2021_11_25 +10:00, 2135_11_25 +13:00

    start_date_obj = datetime.datetime.strptime(start_date, "%Y_%m_%d %z")
    future_date_obj = datetime.datetime.strptime(future_date, "%Y_%m_%d %z")
    print(f"The start date object is: {start_date_obj}")
    print(f"The future date object is: {future_date_obj}")
    # now that the filtering has commenced
    # compare the 2 datetimes to ensure they're the same across the 2 dags
    # the actual comparison
    the_difference = future_date_obj - start_date_obj
    print(f"This is the difference between the 2 start dates in days: {the_difference}")
    # the_difference_yrs = the_difference % 365
    # print(the_difference_yrs)
    # the_difference_mths =


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="scheduler_crash_with_date_args_v2",
    default_args=default_args,
    # pendulum start date, datetime future user defined macro
    start_date=start_date,
    schedule=None,
    catchup=False,
    user_defined_macros={"future_date": pendulum.datetime(2135, 11, 25, tz=pendulum.timezone("Pacific/Chuuk"))},
    tags=["core"],
) as dag:
    t1 = PythonOperator(
        task_id="subtract_future_and_start_date",
        python_callable=subtractor,
        op_args=[str(start_date), "{{ future_date }}"],
    )

t1
