##############################################################################
# Import necessary modules
# #############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

from Lead_scoring_training_pipeline.utils import *

###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,9,1),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)

###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################
task_encode_features = PythonOperator(
    task_id = "encode_features",
    python_callable = encode_features,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################
task_get_trained_model = PythonOperator(
    task_id = "get_trained_model",
    python_callable = get_trained_model,
    dag = ML_data_cleaning_dag
)

###############################################################################
# Define relations between tasks
# ##############################################################################
task_encode_features >> task_get_trained_model
