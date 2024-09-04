##############################################################################
# Import necessary modules
# #############################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils import *

###############################################################################
# Define default arguments and create an instance of DAG
# ##############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,9,1),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


Lead_scoring_inference_dag = DAG(
                dag_id = 'Lead_scoring_inference_pipeline',
                default_args = default_args,
                description = 'Inference pipeline of Lead Scoring system',
                schedule_interval = '@hourly',
                catchup = False
)

###############################################################################
# Create a task for encode_data_task() function with task_id 'encoding_categorical_variables'
# ##############################################################################
task_encode_features = PythonOperator(
    task_id = "encode_features",
    python_callable = encode_features,
    dag = Lead_scoring_inference_dag
)


###############################################################################
# Create a task for load_model() function with task_id 'generating_models_prediction'
# ##############################################################################
task_get_models_prediction = PythonOperator(
    task_id = "get_models_prediction",
    python_callable = get_models_prediction,
    dag = Lead_scoring_inference_dag
)


###############################################################################
# Create a task for prediction_col_check() function with task_id 'checking_model_prediction_ratio'
# ##############################################################################
task_prediction_ratio_check = PythonOperator(
    task_id = "prediction_ratio_check",
    python_callable = prediction_ratio_check,
    dag = Lead_scoring_inference_dag
)


###############################################################################
# Create a task for input_features_check() function with task_id 'checking_input_features'
# ##############################################################################
task_input_features_check = PythonOperator(
    task_id = "input_features_check",
    python_callable = input_features_check,
    dag = Lead_scoring_inference_dag
)


###############################################################################
# Define relation between tasks
# ##############################################################################
task_encode_features >> task_input_features_check >> task_get_models_prediction >> task_prediction_ratio_check
