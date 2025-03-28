from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime

# Define a dataset
sample_dataset = Dataset("s3://my-bucket/sample-data.csv")

def create_sample_data():
    # Creating a simple dataset
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [85, 90, 78]
    }
    df = pd.DataFrame(data)
    
    # Save dataset (mocking save to S3 or DB)
    df.to_csv("/tmp/sample-data.csv", index=False)

with DAG(
    dag_id="sample_dataset_dag_which_triggers_another_dag",
    schedule=None,
    catchup=False,
    tags=["dataset", "example"],
    start_date=datetime(2024, 1, 1)
) as dag:
    
    create_dataset_task = PythonOperator(
        task_id="create_dataset",
        python_callable=create_sample_data,
        outlets=[sample_dataset],  # Marking dataset as an output
    )

# Second DAG triggered by dataset update
with DAG(
    dag_id="dataset_triggered_dag1",
    schedule=[sample_dataset],  # This DAG is triggered by dataset updates
    catchup=False,
    tags=["dataset", "triggered"],
    start_date=datetime(2024, 1, 1)
) as triggered_dag:
    
    def process_sample_data():
        # Mock processing logic
        df = pd.read_csv("/tmp/sample-data.csv")
        print("Processing dataset:")
        print(df)
    
    process_data_task = PythonOperator(
        task_id="process_dataset",
        python_callable=process_sample_data,
    )
