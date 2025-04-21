from airflow.decorators import dag, task
from pendulum import today


@dag(
    schedule="@daily",
    start_date=today("UTC").add(days=-1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["shrd-task-vol"],
)
def example_dag_with_shared_task_volume():
    """
    ### Basic Dag
    This is a basic dag with task using shared data volume to communicate
    """

    @task()
    def write():
        """
        #### Write task
        A simple "write" task to write data to /usr/local/airflow/data/test.txt file, wherein
        /usr/local/airflow/data is shared volume between tasks.
        """
        data_string = "test data string"

        file = open("/usr/local/airflow/data/test.txt", "w+")
        file.write(data_string)
        file.close()

    @task()
    def read():
        """
        #### Read task
        A simple "read" task which reads file at /usr/local/airflow/data/test.txt
        and compare the file content to expected string.
        """

        file = open("/usr/local/airflow/data/test.txt")
        file_data = file.read()
        file.close()
        assert file_data == "test data string"

    write = write()
    read = read()


example_dag_with_shared_task_volume = example_dag_with_shared_task_volume()
