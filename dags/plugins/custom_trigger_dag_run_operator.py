import datetime
import time
from time import sleep

from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagNotFound, DagRunAlreadyExists
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.types import DagRunType

XCOM_EXECUTION_DATE_ISO = "trigger_execution_date_iso"
XCOM_RUN_IDS = "trigger_run_id"


class CustomTriggerDagRunOperator(TriggerDagRunOperator):
    """
    Init vars
    - num_dag_triggers: Number of dag runs to trigger for specified dag
    """

    def __init__(self, num_dag_triggers, *args, **kwargs):
        self.num_dag_triggers = num_dag_triggers
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        first_dag_run = None
        triggered_dag_run_ids = []
        for item in range(self.num_dag_triggers):
            sleep(0.3)

            if isinstance(self.execution_date, datetime.datetime):
                parsed_execution_date = self.execution_date
            elif isinstance(self.execution_date, str):
                parsed_execution_date = timezone.parse(self.execution_date)
            else:
                parsed_execution_date = timezone.utcnow()

            if self.trigger_run_id:
                run_id = self.trigger_run_id
            else:
                run_id = DagRun.generate_run_id(DagRunType.MANUAL, parsed_execution_date)
            try:
                dag_run = trigger_dag(
                    dag_id=self.trigger_dag_id,
                    run_id=run_id,
                    conf=self.conf,
                    execution_date=parsed_execution_date,
                    replace_microseconds=False,
                )

            except DagRunAlreadyExists as e:
                if self.reset_dag_run:
                    self.log.info("Clearing %s on %s", self.trigger_dag_id, parsed_execution_date)

                    # Get target dag object and call clear()

                    dag_model = DagModel.get_current(self.trigger_dag_id)
                    if dag_model is None:
                        raise DagNotFound(f"Dag id {self.trigger_dag_id} not found in DagModel")

                    dag_bag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
                    dag = dag_bag.get_dag(self.trigger_dag_id)
                    dag.clear(start_date=parsed_execution_date, end_date=parsed_execution_date)
                    dag_run = DagRun.find(dag_id=dag.dag_id, run_id=run_id)[0]
                else:
                    raise e
            if dag_run is None:
                raise RuntimeError("The dag_run should be set here!")
            # Store the execution date from the dag run (either created or found above) to
            # be used when creating the extra link on the webserver.
            ti = context["task_instance"]
            ti.xcom_push(key=XCOM_EXECUTION_DATE_ISO, value=dag_run.execution_date.isoformat())
            triggered_dag_run_ids.append(dag_run.run_id)
            if item == 0:
                first_dag_run = dag_run
        ti.xcom_push(key=XCOM_RUN_IDS, value=triggered_dag_run_ids)

        if self.wait_for_completion:
            # wait for first triggered dag run to reach an allowed state
            while True:
                self.log.info(
                    "Waiting for %s on %s to become allowed state %s ...",
                    self.trigger_dag_id,
                    first_dag_run.execution_date,
                    self.allowed_states,
                )
                time.sleep(self.poke_interval)
                first_dag_run.refresh_from_db()
                state = first_dag_run.state
                # Only wait for the first dagrun to move to a running state then continue with execution
                if state in self.allowed_states:
                    self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
                    return
