from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
import pandas as pd

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
    dag_id="sample_dataset_dag",
    schedule=None,
    catchup=False,
    tags=["dataset", "example"],
) as dag:
    
    create_dataset_task = PythonOperator(
        task_id="create_dataset",
        python_callable=create_sample_data,
        outlets=[sample_dataset],  # Marking dataset as an output
    )
    
    create_dataset_task
