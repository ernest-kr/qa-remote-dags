from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import time

# Function for Task 1: Data extraction (lightweight task)
def extract_data():
    print("Extracting data from API...")
    time.sleep(2)  # Simulating API call delay
    # Simulate successful data extraction
    data = {'raw_data': random.randint(1, 100)}
    print(f"Extracted Data: {data}")
    return data

# Function for Task 2: Data transformation (medium-weight task)
def transform_data(extracted_data):
    print("Transforming data...")
    time.sleep(5)  # Simulating transformation delay
    # Simulate a transformation (data normalization, cleaning)
    transformed_data = {'transformed_data': extracted_data['raw_data'] * 2}
    print(f"Transformed Data: {transformed_data}")
    return transformed_data

# Function for Task 3: Machine learning model training (resource-heavy task)
def train_model(transformed_data):
    print("Training model with transformed data...")
    time.sleep(10)  # Simulating model training delay
    # Simulate model training process
    model_accuracy = random.uniform(0.8, 0.95)
    print(f"Model trained with accuracy: {model_accuracy}")
    return model_accuracy

# Define the DAG
dag = DAG(
    'realistic_data_processing_pipeline',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'start_date': datetime(2025, 3, 26),
    }, # No schedule, can be triggered manually
    catchup=False,
)

# Task 1: Data extraction (Lightweight task, Target 'queue_1')
task_1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
    queue='queue-1',  # Lightweight task will run on workers listening to 'queue_1'
)

# Task 2: Data transformation (Medium-weight task, Target 'queue_2')
task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[task_1.output],  # Pass extracted data as input
    dag=dag,
    queue='queue-2',  # Medium-weight task will run on workers listening to 'queue_2'
)

# Task 3: Model training (Heavy computation, Target 'queue_3')
task_3 = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    op_args=[task_2.output],  # Pass transformed data as input
    dag=dag,
    queue='queue-3',  # Resource-heavy task will run on workers listening to 'queue_3'
)

# Set task dependencies (task flow)
task_1 >> task_2 >> task_3
