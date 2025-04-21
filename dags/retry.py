from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.sdk import DAG
from airflow.triggers.testing import SuccessTrigger

# https://github.com/apache/airflow/issues/18146#issuecomment-918519695


class RetryOperator(BaseOperator):
    def execute(self, context):
        ti = context["ti"]
        has_next_method = bool(ti.next_method)
        try_number = ti.try_number
        self.log.info(f"In `execute`: has_next_method: {has_next_method}, try_number:{try_number}")

        self.defer(
            trigger=SuccessTrigger(),
            method_name="next",
            kwargs={"execute_try_number": try_number},
        )

    def next(self, context, execute_try_number, event=None):
        self.log.info("In next!")
        ti = context["ti"]
        has_next_method = bool(ti.next_method)
        try_number = ti.try_number
        self.log.info(
            f"In `next`: has_next_method: {has_next_method}, try_number:{try_number}, excute_try_number: {execute_try_number}"
        )

        if try_number == 1:
            # Force a retry
            raise AirflowException("Force a retry")
        # Did we run `execute`?
        if execute_try_number != try_number:
            raise AirflowException("`execute` wasn't run during retry!")
        return None  # Success!


with DAG("triggerer_retry", schedule=None, start_date=datetime(2021, 9, 13), tags=["core"]) as dag:
    RetryOperator(task_id="retry", retries=1, retry_delay=timedelta(seconds=15))
